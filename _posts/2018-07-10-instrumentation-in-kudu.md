---
layout: post
title: Instrumentation in Apache Kudu
author: Todd Lipcon
---

Last week, the [OpenTracing](http://opentracing.io/) community invited me to
their monthly Google Hangout meetup to give an informal talk on tracing and
instrumentation in Apache Kudu.

While Kudu doesn't currently support distributed tracing using OpenTracing,
it does have quite a lot of other types of instrumentation, metrics, and
diagnostics logging. The OpenTracing team was interested to hear about some of
the approaches that Kudu has used, and so I gave a brief introduction to topics
including:
<!--more-->
- The Kudu [diagnostics log](/docs/administration.html#_diagnostics_logging)
  which periodically logs metrics and stack traces.
- The [process-wide tracing](/docs/troubleshooting.html#kudu_tracing)
  support based on the open source tracing framework implemented by Google Chrome.
- The [stack watchdog](/docs/troubleshooting.html#kudu_tracing)
  which helps us find various latency outliers and issues in our libraries and
  the Linux kernel.
- [Heap sampling](/docs/troubleshooting.html#heap_sampling) support
  which helps us understand unexpected memory usage.

If you're interested in learning about these topics and more, check out the video recording
below. My talk spans the first 34 minutes.

<iframe width="800" height="500"
  src="https://www.youtube.com/embed/qBXwKU6Ubjo?end=2058&start=23">
</iframe>

If you have any questions about this content or about Kudu in general,
[join the community](http://kudu.apache.org/community.html)
