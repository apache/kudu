// Copyright (c) 2013, Cloudera, inc.
//
// Simple pool/freelist for objects of the same type, typically used
// in local context.
#ifndef KUDU_UTIL_OBJECT_POOL_H
#define KUDU_UTIL_OBJECT_POOL_H

#include <glog/logging.h>
#include <stdint.h>
#include "gutil/manual_constructor.h"

namespace kudu {

using base::ManualConstructor;


// An object pool allocates and destroys a single class of objects
// off of a free-list.
//
// Upon destruction of the pool, any objects allocated from this pool are
// destroyed, regardless of whether they have been explicitly returned to the
// pool.
//
// This class is similar to the boost::pool::object_pool, except that the boost
// implementation seems to have O(n) deallocation performance and benchmarked
// really poorly.
//
// This class is not thread-safe.
template<typename T>
class ObjectPool {
public:
  ObjectPool() :
    free_list_head_(NULL),
    alloc_list_head_(NULL) {
  }

  ~ObjectPool() {
    // Delete all objects ever allocated from this pool
    ListNode *node = alloc_list_head_;
    while (node != NULL) {
      ListNode *tmp = node;
      node = node->next_on_alloc_list;
      if (!tmp->is_on_freelist) {
        // Have to run the actual destructor if the user forgot to free it.
        tmp->obj.Destroy();
      }
      delete tmp;
    }
  }

  // Construct a new object instance from the pool.
  T *Construct() {
    ManualConstructor<T> *obj = GetObject();
    obj->Init();
    return obj->get();
  }

  // Destroy an object, running its destructor and returning it to the
  // free-list.
  void Destroy(T *t) {
    CHECK_NOTNULL(t);
    uintptr_t t_ptr = reinterpret_cast<uintptr_t>(t);
    t_ptr -= offsetof(ListNode, obj);
    ListNode *node = reinterpret_cast<ListNode *>(t_ptr);

    node->obj.Destroy();

    DCHECK(!node->is_on_freelist);
    node->is_on_freelist = true;
    node->next_on_free_list = free_list_head_;
    free_list_head_ = node;
  }

private:
  struct ListNode {
    ListNode() : next_on_free_list(NULL),
                 next_on_alloc_list(NULL),
                 is_on_freelist(false)
    {}

    ManualConstructor<T> obj;
    ListNode *next_on_free_list;
    ListNode *next_on_alloc_list;

    bool is_on_freelist;
  };


  ManualConstructor<T> *GetObject() {
    if (free_list_head_ != NULL) {
      ListNode *tmp = free_list_head_;
      free_list_head_ = tmp->next_on_free_list;
      tmp->next_on_free_list = NULL;
      DCHECK(tmp->is_on_freelist);
      tmp->is_on_freelist = false;

      return &tmp->obj;
    }
    ListNode *new_node = new ListNode();
    new_node->next_on_alloc_list = alloc_list_head_;
    alloc_list_head_ = new_node;
    return &new_node->obj;
  }

  // Keeps track of free objects in this pool.
  ListNode *free_list_head_;

  // Keeps track of all objects ever allocated by this pool.
  ListNode *alloc_list_head_;
};


} // namespace kudu
#endif
