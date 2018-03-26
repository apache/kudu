// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <memory>
#include <utility>

// It isn't possible to use 'std::make_shared' on a class with private or protected
// constructors. Using friends as a workaround worked in some earlier libc++/libstdcxx
// versions, but in the latest versions there are some static_asserts that seem to defeat
// this trickery. So, instead, we rely on the "curiously recurring template pattern" (CRTP)
// to inject a static 'make_shared' function inside the class.
//
// See https://stackoverflow.com/questions/8147027/how-do-i-call-stdmake-shared-on-a-class-with-only-protected-or-private-const
// for some details.
//
// Usage:
//
//  class MyClass : public enable_make_shared<MyClass> {
//   public:
//     ...
//
//   protected:
//    // The constructor must be protected rather than private.
//    MyClass(Foo arg1, Bar arg2) {
//    }
//
//  }
//
//    shared_ptr<MyClass> foo = MyClass::make_shared(arg1, arg2);
template<class T>
class enable_make_shared { // NOLINT
 public:

  // Define a static make_shared member which constructs the public subclass
  // and casts it back to the desired class.
  template<typename... Arg>
  static std::shared_ptr<T> make_shared(Arg&&... args) {
    // Define a struct subclass with a public constructor which will be accessible
    // from make_shared.
    struct make_shared_enabler : public T { // NOLINT
      explicit make_shared_enabler(Arg&&... args) : T(std::forward<Arg>(args)...) {
      }
    };

    return ::std::make_shared<make_shared_enabler>(
        ::std::forward<Arg>(args)...);
  }
};
