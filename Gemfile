# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

source 'https://rubygems.org'

# Explicitly use the 'kramdown' markdown variant. Otherwise
# the TOC generation for the FAQ doesn't work.
gem 'kramdown'

# We pin a slightly older version of jekyll which only requires
# Ruby 1.9. Jekyll 3.0 requires Ruby 2.0 which is more painful to
# install on some hosts.
gem 'jekyll', '~> 2.5.3'

# Jekyll requires a Javascript runtime:
# https://github.com/jekyll/jekyll/issues/2327
gem 'therubyracer' # V8 runtime installer for Jekyll

# Produce /feed.xml for the Jekyll blog.
gem 'jekyll-feed'

# Used for 'site_tool proof'.
# Pin to an old version which doesn't require ruby 2.0.
gem 'html-proofer', '~> 2.6.4'

gem 'fileutils', '~> 1.0.2'
gem 'date', '~> 1.0.0'
