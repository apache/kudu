{% extends 'markdown.tpl' %}

{%- block header -%}
---
layout: post
title: "{{resources['metadata']['name']}}"
---
{%- endblock header -%}

{% block input %}
{{ '{% highlight python %}' }}
{{ cell.source }}
{{ '{% endhighlight %}' }}
{% endblock input %}

{% block data_png %}
![png]({{ output.metadata.filenames['image/png'] | path2url }}){: .img-responsive}
{% endblock data_png %}

{% block data_markdown scoped %}
{{ output.data['text/markdown'] }}
{% endblock data_markdown %}
