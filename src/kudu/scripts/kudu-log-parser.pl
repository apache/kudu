#!/usr/bin/perl
################################################################################
# Kudu log parser. Summarize delays potentially related to RaftConsensus
# activity across a cluster.
# Pass it a list of logs from a whole cluster.
# The logs are allowed to be gzipped.
################################################################################
use strict;
use warnings;

use Getopt::Long qw(GetOptions);
use Data::Dumper qw(Dumper);
use DateTime;
use DateTime::Format::Strptime;
use Date::Parse qw(str2time);

sub main();
sub process_file($$$);
sub print_history(@);
sub print_record($$);
sub duration_to_nanos($);
sub glog_timestamp_to_micros($$);
sub timestamp_string_to_micros($);
sub format_ts($);
sub format_duration_micros($);

# Extension of gzip files.
my $pat_gzip_ext = qr/\.gz$/;

# Log line patterns
# -----------------
my $pat_glog_prefix = qr{^[A-Z](\d{4}) (\d{2}:\d{2}:\d{2}\.\d{6})\s+\d+ \S+\]};
my $pat_tablet_prefix = qr{$pat_glog_prefix T (\S+) P (\S+)};
my $pat_raft_prefix = qr{$pat_tablet_prefix \[term (\d+) (\w+)\]:};
my $pat_raft_election_prefix = qr{$pat_tablet_prefix \[CANDIDATE\]: Term (\d+) ((?:pre-)?election):};

# W0912 11:19:51.896980 30674 consensus_peers.cc:365] T 45765f7e72ae4923aa5ba2a7df69fabf P b80b06235d304dad93ada855a442e051 -> Peer 5b25d072a0864275806370a4834f1dad (foo.example.com:7050): Couldn't send request to peer 5b25d072a0864275806370a4834f1dad for tablet 45765f7e72ae4923aa5ba2a7df69fabf. Status: Timed out: UpdateConsensus RPC to 10.0.0.7:7050 timed out after 1.000s (SENT). Retrying in the next heartbeat period. Already tried 4 times.
my $pat_raft_hb_timeout = qr{$pat_tablet_prefix -> Peer (\S+) (\(\S+\)): .* Timed out: UpdateConsensus RPC .* timed out after ([\d.]+)s};

# I0926 11:16:07.158995  3293 raft_consensus.cc:436] T 3480ca98bb444244b73fe9ee082d1449 P b80b06235d304dad93ada855a442e051 [term 42155 FOLLOWER]: Starting leader election with config: opid_index: 31577 OBSOLETE_local: false peers { permanent_uuid: "a283d70a0bfb4ef8a324e4f0893bfa2d" member_type: VOTER last_known_addr { host: "host1.example.com" port: 7050 } } peers { permanent_uuid: "5b25d072a0864275806370a4834f1dad" member_type: VOTER last_known_addr { host: "host2.example.com" port: 7050 } } peers { permanent_uuid: "b80b06235d304dad93ada855a442e051" member_type: VOTER last_known_addr { host: "host3.example.com" port: 7050 } }
my $pat_raft_start_election = qr{$pat_raft_prefix Starting (?:(?:forced )?leader )?((?:pre-)?election) \(([^)]+)\)$};

# I0926 11:17:05.697587 15122 leader_election.cc:243] T 45765f7e72ae4923aa5ba2a7df69fabf P 5b25d072a0864275806370a4834f1dad [CANDIDATE]: Term 37134 pre-election: Election decided. Result: candidate won.
# I0926 11:17:05.697796 15121 leader_election.cc:243] T efc242c62e0746e9b6514fc67d90daac P 5b25d072a0864275806370a4834f1dad [CANDIDATE]: Term 29786 pre-election: Election decided. Result: candidate lost.
# I0118 19:08:21.448396 26370 leader_election.cc:258] T 961748f097f64ff09b3e803a1fdff15e P 23d473f441674d43807fd9e631862bfd [CANDIDATE]: Term 2010 election: Election decided. Result: candidate won.
my $pat_raft_election_result = qr{$pat_raft_election_prefix Election decided\. Result: candidate (\w+)\.};

# W0926 11:17:05.697788 15121 leader_election.cc:272] T efc242c62e0746e9b6514fc67d90daac P 5b25d072a0864275806370a4834f1dad [CANDIDATE]: Term 29786 pre-election: RPC error from VoteRequest() call to peer a283d70a0bfb4ef8a324e4f0893bfa2d: Timed out: RequestConsensusVote RPC to 10.0.0.50:7050 timed out after 1.512s (SENT)
my $pat_raft_election_timeout = qr{$pat_raft_election_prefix RPC error from VoteRequest\(\) call to peer (\w+): (.*)$};

# I0926 11:18:08.630019 10465 raft_consensus.cc:519] T 6082f57e00094402a7ff15ae4795dac5 P b80b06235d304dad93ada855a442e051 [term 41550 LEADER]: Becoming Leader. State: Replica: b80b06235d304dad93ada855a442e051, State: 1, Role: LEADER
my $pat_raft_becoming_leader = qr{$pat_raft_prefix Becoming Leader\.};

# I0912 11:19:51.976449 30979 raft_consensus.cc:1861] T fa265673c946476592e781921b90e8c3 P b80b06235d304dad93ada855a442e051 [term 21610 LEADER]: Leader pre-election vote request: Denying vote to candidate 5b25d072a0864275806370a4834f1dad for term 21611 because replica is either leader or believes a valid leader to be alive.
my $pat_raft_deny_vote = qr{$pat_raft_prefix Leader (\S+) vote request: Denying vote to candidate (\S+) for term (\d+) because (.*?)[.]?$};

# I0926 11:16:11.727576 15199 raft_consensus.cc:1900] T 40e5b8a7ba8d4770a8eadcdae0c210c8 P 5b25d072a0864275806370a4834f1dad [term 18965 FOLLOWER]: Leader pre-election vote request: Granting yes vote for candidate a283d70a0bfb4ef8a324e4f0893bfa2d in term 18965.
my $pat_raft_grant_vote = qr{$pat_raft_prefix Leader (\S+) vote request: Granting yes vote for candidate (\S+) in term (\d+)\.$};

# W0926 11:18:56.379592 26278 outbound_call.cc:232] RPC callback for RPC call kudu.consensus.ConsensusService.UpdateConsensus -> {remote=10.0.0.50:7050, user_credentials={real_user=kudu}} blocked reactor thread for 50364.1us
my $pat_blocked_reactor = qr{$pat_glog_prefix RPC callback for RPC call (\S+) -> \{remote=(\S+), user_credentials=\{real_user=kudu\}\} blocked reactor thread for (\S+)$};

# W0926 11:17:05.677790 15105 kernel_stack_watchdog.cc:146] Thread 15122 stuck at /home/mpercy/src/kudu/src/kudu/rpc/outbound_call.cc:218 for 166ms:
my $pat_kernel_stack_watchdog = qr{$pat_glog_prefix Thread (\d+) stuck at .*/([^/]+\.\w+):(\d+) for (\S+):$};

# W0926 11:19:01.339553 27231 net_util.cc:129] Time spent resolving address for foo1.example.com: real 0.202s    user 0.000s     sys 0.000s
my $pat_slow_execution = qr{$pat_glog_prefix .*(Time spent .*): real (\S+)};

# Global datetime formatter object.
my $g_dt_formatter = DateTime::Format::Strptime->new(pattern => "%m/%d %H:%M:%S.%6N");

use constant RAW_LOG_LINE      => '';
use constant RAFT_UPDT_TIMEOUT => 'RAFT_UPDT_TIMEOUT';
use constant BECOMING_LEADER   => 'BECOMING_LEADER  ';
use constant ELECTION_STARTING => 'ELECTION_START   ';
use constant VOTE              => 'VOTE             ';
use constant BLOCKED_REACTOR   => 'BLOCKED_REACTOR  ';
use constant KERNEL_DELAY      => 'KERNEL_DELAY     ';
use constant SLOW_EXECUTION    => 'SLOW_EXECUTION   ';
use constant ELECTION_RESULT   => 'ELECTION_RESULT  ';
use constant VOTE_REQ_TIMEOUT  => 'VOTE_REQ_TIMEOUT ';

########################################################################

my $rc = main();
exit $rc;

########################################################################

sub main() {
  if (!@ARGV) {
    print STDERR "Usage: $0 [options] <glog1> [<glog2> ...]\n";
    print STDERR "Where the following options are available:\n";
    print STDERR "  --tablet <tablet-id>      Filter by tablet id\n";
    print STDERR "  --nosummary               Print raw log lines\n";
    print STDERR "  --before <ts>             Only include lines with timestamp earlier than or equal to <ts>\n";
    print STDERR "  --after <ts>              Only include lines with timestamp later than or equal to <ts>\n";
    print STDERR "Note that many formats are supported for <ts>. See 'perldoc Date::Parse' for details.\n";
    return 1;
  }

  my %opts;
  GetOptions(\%opts,
             "tablet=s",
             "nosummary",
             "before=s",
             "after=s") or die "Error in command-line arguments: $!";

  my $tot_lines = 0;
  my @records = ();
  foreach my $file (@ARGV) {
    $tot_lines += process_file($file, \%opts, \@records);
  }
  #print STDERR "Number of lines: $tot_lines\n";
  #print STDERR "Number of records: " . scalar @records . "\n";
  print_history(@records);

  return 0;
}

sub process_file($$$) {
  defined(my $file = shift) or die;
  defined(my $opts = shift) or die;
  defined(my $records = shift) or die;

  # Support gzipped files.
  if ($file =~ $pat_gzip_ext) {
    open(IN, "gunzip -c $file |") or die "can't open gunzip pipe to $file: $!";
  } else {
    open(IN, "< $file") or die "can't open $file: $!";
  }

  print STDERR "processing file $file...\n";

  my $min_ts_micros = 0;
  my $max_ts_micros = 2**64 - 1;

  if (exists $opts->{before}) {
    $max_ts_micros = timestamp_string_to_micros($opts->{before});
  }
  if (exists $opts->{after}) {
    $min_ts_micros = timestamp_string_to_micros($opts->{after});
  }

  my $line_count = 0;
  while (defined(my $line = <IN>)) {
    $line_count++;
    # Skip non-glog lines.
    next unless $line =~ $pat_glog_prefix;
    my ($date, $ts) = ($1, $2);
    my $ts_micros = glog_timestamp_to_micros($date, $ts);
    next unless $ts_micros >= $min_ts_micros;
    next unless $ts_micros <= $max_ts_micros;

    chomp $line;

    if (exists $opts->{nosummary}) {
      if (exists $opts->{tablet}) {
        next unless (my @m = $line =~ $pat_tablet_prefix);
        my ($date, $ts, $tablet_id, $local_peer_uuid) = @m;
        next unless $tablet_id eq $opts->{tablet};
      }
      push @$records, {ts => $ts_micros, type => RAW_LOG_LINE(),
                       data => {line => $line}};
      next;
    }

    if (my @m = $line =~ $pat_raft_hb_timeout) {
      my ($date, $ts, $tablet_id, $local_peer_uuid, $remote_peer_uuid, $remote_peer_host, $timeout_sec) = @m;
      next if exists $opts->{tablet} && $tablet_id ne $opts->{tablet};
      push @$records, {ts => $ts_micros, type => RAFT_UPDT_TIMEOUT(),
                       data => {tablet_id => $tablet_id, local_peer_uuid => $local_peer_uuid,
                               remote_peer_uuid => $remote_peer_uuid}};
      next;
    }

    if (my @m = $line =~ $pat_raft_start_election) {
      my ($date, $ts, $tablet_id, $local_peer_uuid, $local_peer_term, $local_peer_role, $election_type, $reason) = @m;
      next if exists $opts->{tablet} && $tablet_id ne $opts->{tablet};
      push @$records, {ts => $ts_micros, type => ELECTION_STARTING(),
                       data => {tablet_id => $tablet_id, local_peer_uuid => $local_peer_uuid,
                               local_peer_term => $local_peer_term, local_peer_role => $local_peer_role,
                               election_type => $election_type, reason => $reason}};
      next;
    }

    if (my @m = $line =~ $pat_raft_election_result) {
      my ($date, $ts, $tablet_id, $local_peer_uuid, $local_peer_term, $election_type, $result) = @m;
      next if exists $opts->{tablet} && $tablet_id ne $opts->{tablet};
      push @$records, {ts => $ts_micros, type => ELECTION_RESULT(),
                       data => {tablet_id => $tablet_id, local_peer_uuid => $local_peer_uuid,
                               local_peer_term => $local_peer_term, election_type => $election_type,
                               result => $result}};
      next;
    }

    if (my @m = $line =~ $pat_raft_election_timeout) {
      my ($date, $ts, $tablet_id, $local_peer_uuid, $local_peer_term, $election_type, $remote_peer_uuid, $error) = @m;
      next if exists $opts->{tablet} && $tablet_id ne $opts->{tablet};
      push @$records, {ts => $ts_micros, type => VOTE_REQ_TIMEOUT(),
                       data => {tablet_id => $tablet_id, local_peer_uuid => $local_peer_uuid,
                               local_peer_term => $local_peer_term,  election_type => $election_type,
                               remote_peer_uuid => $remote_peer_uuid,
                               error => $error}};
      next;
    }

    if (my @m = $line =~ $pat_raft_becoming_leader) {
      my ($date, $ts, $tablet_id, $local_peer_uuid, $local_peer_term, $local_peer_role) = @m;
      next if exists $opts->{tablet} && $tablet_id ne $opts->{tablet};
      push @$records, {ts => $ts_micros, type => BECOMING_LEADER(),
                       data => {tablet_id => $tablet_id, local_peer_uuid => $local_peer_uuid,
                               local_peer_term => $local_peer_term, local_peer_role => $local_peer_role}};
      next;
    }

    if (my @m = $line =~ $pat_raft_deny_vote) {
      next; # We don't really care about denied votes since they are not very expensive
      my ($date, $ts, $tablet_id, $local_peer_uuid, $local_peer_term, $local_peer_role,
          $election_type, $remote_peer_uuid, $remote_peer_term, $reason) = @m;
      next if exists $opts->{tablet} && $tablet_id ne $opts->{tablet};
      push @$records, {ts => $ts_micros, type => VOTE(),
                       data => {tablet_id => $tablet_id, local_peer_uuid => $local_peer_uuid,
                               local_peer_term => $local_peer_term, local_role => $local_peer_role,
                               election_type => $election_type, remote_peer_uuid => $remote_peer_uuid,
                               remote_peer_term => $remote_peer_term, reason => $reason, result => "DENIED"}};
      next;
    }

    if (my @m = $line =~ $pat_raft_grant_vote) {
      my ($date, $ts, $tablet_id, $local_peer_uuid, $local_peer_term, $local_peer_role, $election_type,
          $remote_peer_uuid, $remote_peer_term) = @m;
      next if exists $opts->{tablet} && $tablet_id ne $opts->{tablet};
      push @$records, {ts => $ts_micros, type => VOTE(),
                       data => {tablet_id => $tablet_id, local_peer_uuid => $local_peer_uuid,
                               local_peer_term => $local_peer_term, local_role => $local_peer_role,
                               election_type => $election_type, remote_peer_uuid => $remote_peer_uuid,
                               remote_peer_term => $remote_peer_term, reason => "", result => "GRANTED"}};
      next;

    }

    if (my @m = $line =~ $pat_blocked_reactor) {
      my ($date, $ts, $rpc, $remote_addr, $duration_str) = @m;
      my $duration_micros = duration_to_micros($duration_str);
      push @$records, {ts => $ts_micros, type => BLOCKED_REACTOR(),
                       data => {rpc => $rpc, remote_addr => $remote_addr, duration_micros => $duration_micros}};
      next;
    }

    if (my @m = $line =~ $pat_kernel_stack_watchdog) {
      my ($date, $ts, $thread, $file, $line, $duration_str) = @m;
      my $duration_micros = duration_to_micros($duration_str);
      push @$records, {ts => $ts_micros, type => KERNEL_DELAY(),
                       data => {thread => $thread, file => $file, line => $line, duration_micros => $duration_micros}};
      next;
    }

    if (my @m = $line =~ $pat_slow_execution) {
      my ($date, $ts, $task, $duration_str) = @m;
      my $duration_micros = duration_to_micros($duration_str);
      push @$records, {ts => $ts_micros, type => SLOW_EXECUTION(),
                       data => {task => $task, duration_micros => $duration_micros}};
      next;
    }
  }

  return $line_count;
}

sub print_history(@) {
  # TODO(mpercy): sort individual files and merge them at the end.
  @_ = sort { $a->{ts} <=> $b->{ts} } @_;
  my $first_dup_record = undef;
  my $dup_rec_count = undef;
  foreach my $record (@_) {
    # Accumulate consecutive duplicates.
    if (defined $first_dup_record &&
        ($record->{type} eq RAFT_UPDT_TIMEOUT || $record->{type} eq VOTE || $record->{type} eq ELECTION_STARTING) &&
         $record->{type} eq $first_dup_record->{type} &&
         $record->{data}{tablet_id} eq $first_dup_record->{data}{tablet_id}) {
      # It's a dup.
      ++$dup_rec_count;
      next;
    }
    # Print previous record, if it's there.
    if (defined $first_dup_record) {
      print_record($first_dup_record, $dup_rec_count);
    }
    # Reset the dup count.
    $first_dup_record = $record;
    $dup_rec_count = 1;
  }

  if (defined $first_dup_record) {
    print_record($first_dup_record, $dup_rec_count);
  }
}

sub print_record($$) {
  defined(my $record = shift) or die "Undefined record";
  defined(my $count = shift) or die "Undefined count";
  my $date_str = format_ts($record->{ts});
  my $data = $record->{data};
  if ($record->{type} eq RAW_LOG_LINE) {
    print $data->{line};
  } elsif ($record->{type} eq ELECTION_STARTING) {
    print join("\t", $date_str, $record->{type}, $data->{election_type}, $data->{tablet_id}, $data->{local_peer_uuid},
                     $data->{local_peer_term}, $data->{reason});
  } elsif ($record->{type} eq ELECTION_RESULT) {
    print join("\t", $date_str, $record->{type}, $data->{election_type}, $data->{tablet_id}, $data->{local_peer_uuid},
                     $data->{local_peer_term}, uc($data->{result}));
  } elsif ($record->{type} eq VOTE_REQ_TIMEOUT) {
    print join("\t", $date_str, $record->{type}, $data->{election_type}, $data->{tablet_id}, $data->{local_peer_uuid},
                     $data->{remote_peer_uuid}, $data->{local_peer_term}, $data->{error});
  } elsif ($record->{type} eq BECOMING_LEADER) {
    print join("\t", $date_str, $record->{type}, $data->{tablet_id}, $data->{local_peer_uuid}, $data->{local_peer_term});
  } elsif ($record->{type} eq VOTE) {
    print join("\t", $date_str, $record->{type}, $data->{election_type}, $data->{tablet_id}, $data->{local_peer_uuid},
                     $data->{remote_peer_uuid}, $data->{election_type}, $data->{result}, $data->{reason});
  } elsif ($record->{type} eq RAFT_UPDT_TIMEOUT) {
    print join("\t", $date_str, $record->{type}, $data->{tablet_id}, $data->{local_peer_uuid}, $data->{remote_peer_uuid});
  } elsif ($record->{type} eq BLOCKED_REACTOR) {
    my $duration_str = format_duration_micros($data->{duration_micros});
    print join("\t", $date_str, $record->{type}, $data->{rpc}, $data->{remote_addr}, $duration_str);
  } elsif ($record->{type} eq KERNEL_DELAY) {
    my $duration_str = format_duration_micros($data->{duration_micros});
    print join("\t", $date_str, $record->{type}, $data->{thread}, $data->{file} . ':' . $data->{line}, $duration_str);
  } elsif ($record->{type} eq SLOW_EXECUTION) {
    my $duration_str = format_duration_micros($data->{duration_micros});
    print join("\t", $date_str, $record->{type}, $data->{task}, $duration_str);
  } else {
    die "Unsupported record type: " . $record->{type};
  }
  print " (repeated $count times)" if $count > 1;
  print "\n";
}

# Parse a duration string like "41.2ms" and convert it to microseconds.
sub duration_to_micros($) {
  defined(my $str = shift) or die "Undefined duration";
  if ($str !~ /^([\d.e+-]+)([A-Za-z]+)$/) {
    die "Unsupported duration format: $str";
  }
  my $num = $1 + 0.0;
  my $units = $2;
  if ($units eq 'ns') { return $num / 1000; }
  if ($units eq 'us') { return $num; }
  if ($units eq 'ms') { return $num * 1000; }
  if ($units eq 's' || $units eq 'sec') { return $num * 1000 * 1000; }
  if ($units eq 'm' || $units eq 'min') { return $num * 1000 * 1000 * 60; }
  die "Unsupported units ($units): $str";
}

# Convert the date and timestamp parsed from a glog log prefix (see regular
# expression at the top of this file) to UNIX time in microseconds.
sub glog_timestamp_to_micros($$) {
  defined(my $date = shift) or die;
  defined(my $ts = shift) or die;
  if ($date !~ /^(\d\d)(\d\d)$/) {
    die "Bad date format: $date";
  }
  my ($month, $day) = ($1, $2);
  my $strp = new DateTime::Format::Strptime(
      pattern => '%Y/%m/%d %H:%M:%S.%6N',
      time_zone   => 'UTC', # Arbitrary TZ.
      on_error => 'croak',
  );
  my ($sec, $min, $hour, $mday, $mon, $year, $wday, $yday, $isdst) = localtime();
  $year += 1900;
  my $dt  = $strp->parse_datetime("$year/$month/$day $ts");
  if (!$dt) {
    print STDERR "Warning: Unable to parse date: $date $ts ($month/$day $ts)\n";
    return 0;
  }
  return $dt->epoch() * 1000_000 + $dt->microsecond();
}

# Convert a user-specified timestamp to UNIX time in microseconds.
# For supported date and time formats, see `perldoc Date::Parse`.
sub timestamp_string_to_micros($) {
  defined(my $date = shift) or die;
  my $unixtime = str2time($date);
  return $unixtime * 1000_000; # Return UNIX timestamp in microseconds.
}

sub format_ts($) {
  defined(my $epoch_micros = shift) or die;
  my $dt = DateTime->from_epoch(epoch => $epoch_micros / 1000_000);
  $dt->set_nanosecond($epoch_micros % 1000_000 * 1000);
  return $g_dt_formatter->format_datetime($dt);
}

sub format_duration_micros($) {
  defined(my $duration = shift) or die;
  my $duration_sec = int($duration / 1000_000);
  my $duration_micros = $duration % 1000_000;
  return sprintf("%0d.%06ds", $duration_sec, $duration_micros);
}
