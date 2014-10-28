#!/usr/bin/perl
# Copyright (c) 2014, Cloudera, inc.
# Confidential Cloudera Information: Covered by NDA.
#######################################################################
# This script will convert a stack trace with addresses:
#     @           0x5fb015 kudu::master::Master::Init()
#     @           0x5c2d38 kudu::master::MiniMaster::StartOnPorts()
#     @           0x5c31fa kudu::master::MiniMaster::Start()
#     @           0x58270a kudu::MiniCluster::Start()
#     @           0x57dc71 kudu::CreateTableStressTest::SetUp()
# To one with line numbers:
#     @           0x5fb015 kudu::master::Master::Init() at /home/mpercy/src/kudu/src/master/master.cc:54
#     @           0x5c2d38 kudu::master::MiniMaster::StartOnPorts() at /home/mpercy/src/kudu/src/master/mini_master.cc:52
#     @           0x5c31fa kudu::master::MiniMaster::Start() at /home/mpercy/src/kudu/src/master/mini_master.cc:33
#     @           0x58270a kudu::MiniCluster::Start() at /home/mpercy/src/kudu/src/integration-tests/mini_cluster.cc:48
#     @           0x57dc71 kudu::CreateTableStressTest::SetUp() at /home/mpercy/src/kudu/src/integration-tests/create-table-stress-test.cc:61
#######################################################################
use strict;
use warnings;

if (!@ARGV) {
  die <<EOF
Usage: $0 executable [stack-trace-file]

This script will read addresses from a file containing stack traces and
will convert the addresses that conform to the pattern " @ 0x123456" to line
numbers by calling addr2line on the provided executable.
If no stack-trace-file is specified, it will take input from stdin.
EOF
}

my $binary = shift @ARGV;
if (! -x $binary || ! -r $binary) {
  die "Error: Cannot access executable ($binary)";
}

# Cache lookups to speed processing of files with repeated trace addresses.
my %addr2line_map = ();

# Reading from <ARGV> is magical in Perl.
while (defined(my $input = <ARGV>)) {
  if ($input =~ /^\s+\@\s+(0x[[:xdigit:]]{6,})/) {
    my $addr = $1;
    if (!exists($addr2line_map{$addr})) {
      $addr2line_map{$addr} = `addr2line -ie $binary $addr | tail -1 | awk '{print \$1}'`;
      chomp $addr2line_map{$addr};
    }
    chomp $input;
    $input .= " at " . $addr2line_map{$addr} . "\n";
  }
  print $input;
}

exit 0;
