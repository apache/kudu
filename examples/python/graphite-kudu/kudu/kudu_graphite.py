import sys, os
	
import re
import time
import math
import kudu

from multiprocessing.pool import ThreadPool
from django.conf import settings

import graphite
from graphite.intervals import Interval, IntervalSet
from graphite.node import LeafNode, BranchNode
from graphite.readers import FetchInProgress
from graphite.logger import log

import json

KUDU_MAX_REQUESTS = 10
KUDU_REQUEST_POOL = ThreadPool(KUDU_MAX_REQUESTS)

class KuduNode(object):
    def __init__(self):
        self.child_nodes = []
    
    #Node is leaf, if it has no child nodes.
    def isLeaf(self):
        return len(self.child_nodes) == 0
    
    #Add child node to node.
    def addChildNode(self, node):
        self.child_nodes.append(node)
    
    #Get child node with specified name
    def getChild(self, name):
        for node in self.child_nodes:
            if node.name == name:
                return node
        return None
    
    def getChildren(self):
        return self.child_nodes

    
class KuduTree(KuduNode):
    pass
            

class KuduRegularNode(KuduNode):
    def __init__(self, name):
        KuduNode.__init__(self)
        self.name = name
    
    def getName(self):
        return self.name
 
                
class KuduReader(object):
    __slots__ = ('kudu_table', 'metric_name')
    supported = True

    def __init__(self, kudu_table, metric_name):
        self.kudu_table = kudu_table
        self.metric_name = metric_name

    def get_intervals(self):
        return IntervalSet([Interval(0, time.time())])

    def fetch(self, startTime, endTime):
        def get_data(startTime, endTime):
            log.info("time range %d-%d" % (startTime, endTime))
            host, metric = self.metric_name.split("com.")
            host += "com"
            s = self.kudu_table.scanner()
            s.add_predicate(s.range_predicate(0, host, host))
            s.add_predicate(s.range_predicate(1, metric, metric))
            s.add_predicate(s.range_predicate(2, startTime, endTime))
            s.open()
            values = []
            while s.has_more_rows():
              t = s.next_batch().as_tuples()
              log.info("metric batch: %d" % len(t))
              values.extend([(time, value) for (_, _, time, value) in t])
            # TODO: project just the time and value, not host/metric!
            values.sort()
            values_length = len(values)
            
            if values_length == 0:
                time_info = (startTime, endTime, 1)
                datapoints = []
                return (time_info, datapoints)

            startTime = min(t[0] for t in values)
            endTime = max(t[0] for t in values)
            if values_length == 1:
                time_info = (startTime, endTime, 1)
                datapoints = [values[0][1]]
                return (time_info, datapoints)
            log.info("data: %s" % repr(values))
                    
            # 1. Calculate step (in seconds)
            #    Step will be lowest time delta between values or 1 (in case if delta is smaller)
            step = 1
            minDelta = None
            
            for i in range(0, values_length - 2):
                (timeI, valueI) = values[i]
                (timeIplus1, valueIplus1) = values[i + 1]
                delta = timeIplus1 - timeI
                
                if (minDelta == None or delta < minDelta):
                    minDelta = delta
            
            if minDelta > step:
                step = minDelta
            
            # 2. Fill time info table    
            time_info = (startTime, endTime, step)
            
            # 3. Create array of output points
            number_points = int(math.ceil((endTime - startTime) / step))
            datapoints = [None for i in range(number_points)]
            
            # 4. Fill array of output points
            cur_index = 0
            cur_value = None
            cur_time_stamp = None
            cur_value_used = None
            
            for i in range(0, number_points - 1):
                
                data_point_time_stamp = startTime + i * step
                
                (cur_time_stamp, cur_value) = values[cur_index]
                cur_time_stamp = cur_time_stamp
                
                while (cur_index + 1 < values_length):
                    (next_time_stamp, next_value) = values[cur_index + 1]
                    if next_time_stamp > data_point_time_stamp:
                        break;
                    (cur_value, cur_time_stamp, cur_value_used) = (next_value, next_time_stamp, False)
                    cur_index = cur_index + 1
                    
                data_point_value = None
                if(not cur_value_used and cur_time_stamp <= data_point_time_stamp):
                    cur_value_used = True
                    data_point_value = cur_value
                
                datapoints[i] =  data_point_value
     
            log.info("data: %s" % repr(datapoints))
            return (time_info, datapoints)

        job = KUDU_REQUEST_POOL.apply_async(get_data, [startTime, endTime])
        return FetchInProgress(job.get)
    
    
class KuduFinder(object):
    def __init__(self, kudu_table=None):
        self.client = kudu.Client(settings.KUDU_MASTER)
        self.kudu_table = self.client.open_table(settings.KUDU_TABLE)
        
    # Fills tree of metrics out from flat list
    # of metrics names, separated by dot value
    def _fill_kudu_tree(self, metric_names):
        tree = KuduTree()
        
        for metric_name in metric_names:
            name_parts = re.split("[./]", metric_name)
            
            cur_parent_node = tree
            cur_node = None
            
            for name_part in name_parts:
                cur_node = cur_parent_node.getChild(name_part)
                if cur_node == None:
                    cur_node = KuduRegularNode(name_part)
                    cur_parent_node.addChildNode(cur_node)
                cur_parent_node = cur_node
        
        return tree
    
    
    def _find_nodes_from_pattern(self, kudu_table, pattern):
        query_parts = []
        for part in pattern.split('.'):
            part = part.replace('*', '.*')
            part = re.sub(
                r'{([^{]*)}',
                lambda x: "(%s)" % x.groups()[0].replace(',', '|'),
                part,
            )
            query_parts.append(part)
          
        #Request for metrics
        t = self.client.open_table("metric_ids")
        s = t.scanner()

        # Handle a prefix pattern
        if re.match(".+\\*", pattern):
          prefix_match = pattern[:-1]
          if '.com.' in prefix_match:
            host_prefix, metric_prefix = prefix_match.split(".com.")
            host_prefix += ".com"
            s.add_predicate(s.range_predicate(1, metric_prefix, metric_prefix + "\xff"))
          else:
            host_prefix = prefix_match

          s.add_predicate(s.range_predicate(0, host_prefix, host_prefix + "\xff"))
        elif not "*" in pattern:
          # equality match
          host, metric = pattern.split(".com.")
          host += ".com"
          s.add_predicate(s.range_predicate(0, host, host))
          s.add_predicate(s.range_predicate(1, metric, metric))
        s.open()

        metrics = []
        while s.has_more_rows():
          t = s.next_batch().as_tuples()
          log.info("batch: %d" % len(t))
          metrics.extend(t)
        metric_names = ["%s/%s" % (host, metric) for (host, metric) in metrics]
        #Form tree out of them
        metrics_tree = self._fill_kudu_tree(metric_names)    
        
        for node in self._find_kudu_nodes(kudu_table, query_parts, metrics_tree):
            yield node
    
    def _find_kudu_nodes(self, kudu_table, query_parts, current_branch, path=''):
        query_regex = re.compile(query_parts[0])
        for node, node_data, node_name, node_path in self._get_branch_nodes(kudu_table, current_branch, path):
            dot_count = node_name.count('.') + node_name.count('/')
    
            if dot_count:
                node_query_regex = re.compile(r'\.'.join(query_parts[:dot_count+1]))
            else:
                node_query_regex = query_regex
    
            if node_query_regex.match(node_name):
                if len(query_parts) == 1:
                    yield node
                elif not node.is_leaf:
                    for inner_node in self._find_kudu_nodes(
                        kudu_table,
                        query_parts[dot_count+1:],
                        node_data,
                        node_path,
                    ):
                        yield inner_node
    
    
    def _get_branch_nodes(self, kudu_table, input_branch, path):
        results = input_branch.getChildren()
        if results:
            if path:
                path += '.'
                
            branches = [];
            leaves = [];
            
            for item in results:
                if item.isLeaf():
                    leaves.append(item)
                else:
                    branches.append(item)
            
            if (len(branches) != 0):
                for branch in branches:
                    node_name = branch.getName()
                    node_path = path + node_name
                    yield BranchNode(node_path), branch, node_name, node_path
            if (len(leaves) != 0):
                for leaf in leaves:
                    node_name = leaf.getName()
                    node_path = path + node_name
                    reader = KuduReader(self.kudu_table, node_path)
                    yield LeafNode(node_path, reader), leaf, node_name, node_path

    def find_nodes(self, query):
        log.info("q:" + repr(query))
        try:
          for node in self._find_nodes_from_pattern(self.kudu_table, query.pattern):
              yield node
        except Exception, e:
          log.exception(e)
          raise
