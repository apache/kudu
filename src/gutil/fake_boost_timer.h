#ifndef FAKE_BOOST_TIMER_H
#define FAKE_BOOST_TIMER_H

#include <glog/logging.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <time.h>

namespace boost {
namespace timer {

typedef uint64_t nanosecond_type;

struct cpu_times
{
  nanosecond_type wall;
  nanosecond_type user;
  nanosecond_type system;

  void clear() { wall = user = system = 0LL; }
};

class cpu_timer {
public:
  cpu_timer() :
      stopped_(true)
  {
    times_.clear();
  }

  void start() {
    stopped_ = false;
    GetTimes(&times_);
  }

  void stop() {
    if (stopped_) return;
    stopped_ = true;

    cpu_times current;
    GetTimes(&current);
    times_.wall = current.wall - times_.wall;
    times_.user = current.user - times_.user;
    times_.system = current.system - times_.system;
  }

  cpu_times elapsed() const {
    if (stopped_) return times_;

    cpu_times current;
    GetTimes(&current);
    current.wall -= times_.wall;
    current.user -= times_.user;
    current.system -= times_.system;
    return current;
  }

  void resume() {
    if (!stopped_) return;

    cpu_times current (times_);
    start();
    times_.wall   -= current.wall;
    times_.user   -= current.user;
    times_.system -= current.system;
  }

  bool is_stopped() const {
    return stopped_;
  }

private:
  static void GetTimes(cpu_times *times) {
    struct rusage usage;
    CHECK_EQ(0, getrusage(RUSAGE_THREAD, &usage));
    struct timespec wall;

    CHECK_EQ(0, clock_gettime(CLOCK_MONOTONIC, &wall));
    times->wall   = wall.tv_sec * 1000000000L + wall.tv_nsec;
    times->user   = usage.ru_utime.tv_sec * 1000000000L + usage.ru_utime.tv_usec * 1000;
    times->system = usage.ru_stime.tv_sec * 1000000000L + usage.ru_stime.tv_usec * 1000;

  }

  bool stopped_;

  cpu_times times_;
};

}
}


#endif
