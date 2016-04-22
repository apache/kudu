#!/usr/bin/env python

# This is a template which converts IPython notebooks to Jekyll blog posts
# in markdown.
#
# Snippet based on http://mcwitt.github.io/2015/04/29/jekyll_blogging_with_ipython3/
#
# Usage:
#   ipython nbconvert --config jekyll-ipython-post /path/to/foo.ipynb  --output-dir=../_posts/
#   mv ../posts/foo.md ../posts/2016-04-20-foo.md
#   mv ../posts/foo_files ../img/
#   Edit the markdown file to add your name, title, etc.

try:
    from urllib.parse import quote  # Py 3
except ImportError:
    from urllib2 import quote  # Py 2
import os
import re
import sys

from IPython.nbconvert.preprocessors import Preprocessor

c = get_config()
c.NbConvertApp.export_format = 'markdown'
c.MarkdownExporter.template_file = 'jekyll-ipython-post'
c.Exporter.file_extension = '.md'

def path2url(path):
    """Turn a file path into a URL"""
    parts = path.split(os.path.sep)
    return '{{ site.github.url }}/img/' + '/'.join(quote(part) for part in parts)

class KramdownSyntaxPreprocessor(Preprocessor):
  def preprocess_cell(self, cell, resources, index):
    def replacer(match):
      language, block = match.groups()
      ret = '~~~~' + block + "\n~~~~\n"
      if language:
        ret += "{: .language-%s}" % language
      return ret

    if 'source' in cell and cell.cell_type == 'markdown':
      cell.source, _ = re.subn(r'^```(java)?(.+?)\n```', replacer, cell.source,
                               flags=(re.DOTALL | re.MULTILINE))
    return cell, resources


c.MarkdownExporter.filters = {'path2url': path2url}
c.MarkdownExporter.preprocessors = [KramdownSyntaxPreprocessor]

