// Copyright 2010 Google Inc.  All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
//         dawidk@google.com (Dawid Kurzyniec)
//         wiktor@google.com (Wiktor Tomczak)
//
// A collection of classes (template classes FailureOr, FailureOrOwned,
// and FailureOrReference, and a class FailureOrVoid) to associate
// exceptions with results of function and method calls. It is meant to
// provide exception-alike error propagation, by letting a function to return
// either its normal result, or an 'exception' object indicating abrupt
// completion. See below for the usage pattern.
//
// If your method returns:
// 1) T (by value), then use FailureOr<T>.
// 2) T* not transferring ownership, then use FailureOr<T*>.
// 3) const T* not transferring ownership, then use FailureOr<const T*>.
// 4) T* transferring ownership, then use FailureOrOwned<T>.
// 5) const T* transferring ownership, then use FailureOrOwned<const T>.
// 6) T&, then use FailureOrReference<T>.
// 7) const T&, then use FailureOrReference<const T>.
// 8) void, then use FailureOrVoid.

#ifndef DATAWAREHOUSE_COMMON_EXCEPTION_FAILUREOR_H_
#define DATAWAREHOUSE_COMMON_EXCEPTION_FAILUREOR_H_

// #include "supersonic/supersonic-config.h"


// When the below flag is switched on invoking getter methods on FailureOr<T>
// objects will fail, if the user has not checked the status of the returned
// value beforehand.
#ifndef SUPERSONIC_FORCE_FAILURE_CHECK
# ifndef NDEBUG
#  define SUPERSONIC_FORCE_FAILURE_CHECK 1
# else
#  define SUPERSONIC_FORCE_FAILURE_CHECK 0
# endif
#endif

#include <glog/logging.h>
#include "gutil/logging-inl.h"  // for CHECK macros
#include "gutil/scoped_ptr.h"
#include "gutil/exception/coowned_pointer.h"

namespace common {

// Propagators to support 'exception-like' error propagation. Allow to attach
// exception information to any object passed by value. Intended themselves to
// be passed by value.
//
// Usage pattern:
//
// class Data { ... };
//
// FailureOr<Data> GetSomeData() {
//   if (error detected) {
//     return Failure(new Exception(...));  // Abrupt completion.
//   } else {
//     Data data = ...;
//     return Success(data);                // Normal completion.
//   }
// }
//
// FailureOr<Data> result = GetSomeData();
// if (result.is_failure()) {            // Catch.
//   HandleException(result.exception());
// } else {
//   HandleResult(result.get());
// }
//
// Propagation:
// FailureOr<ManipulatedData> ManipulateSomeData() {
//  FailureOr<Data> data = GetSomeData();  // some function that can fail.
//  if (data.is_failure()) {
//    return Failure(data.release_exception());
//  }
//  ...

struct VoidPropagator {};
template<typename Result> struct ReferencePropagator;
template<typename Result> struct ConstReferencePropagator;
template<typename Exception> struct FailurePropagator;

// Creates an indication o failure. Can be implicitly converted to any
// FailureOr*. Intended to be invoked as part of a return clause from a
// function or method returning one of the FailureOr* classes (see above). In
// that scenario, the ownership of the exception is passed to the resulting
// FailureOr* wrapper.
template<typename Exception>
FailurePropagator<Exception> Failure(Exception* exception);

// Creates an indication of success, with no result. Can be implicitly converted
// to FailureOrVoid.
VoidPropagator Success();

// Create an indication of success, with the specified result. Can be
// implicitly converted to FailureOr<>, FailureOrReference<>,
// and FailureOrOwned<>, as long as there is a valid type conversion from the
// parameter type to the wrapped result type of the FailureOr* class.
// Normally, you don't need to specify the Result type; the type inference does
// its magic. A notable exception is a Success(NULL), which doesn't work because
// NULL is just zero, and you can't cast int to a pointer. In this case, provide
// the pointer type as the Result type.

template<typename Result>
ReferencePropagator<Result> Success(Result& result);  // NOLINT

template<typename Result>
ConstReferencePropagator<Result> Success(const Result& result);

namespace internal {
// A helper class to make it easier to get the right semantics for the
// FailureOrVoid::checked_ field.
class CheckedFlag {
 public:
  CheckedFlag() : value_(false) {}
  // Always reset to false in copies and assignments - the assumption is that
  // copies should be checked again (for example, after returning a
  // FailureOrVoid value from a function...)
  CheckedFlag(const CheckedFlag&) : value_(false) {}
  CheckedFlag& operator= (const CheckedFlag&) {
    value_ = false;
    return *this;
  }
  bool get() const { return value_; }
  void set() { value_ = true; }
 private:
  bool value_;
};
}

// Takes owneship of observed exception.
// A function for conversion between different types of exceptions.
// If ones wants to provide conversion between different type of exceptions he
// must define an explicit constructor of a form:
//
//   explicit Exception(const ObservedException& ss_exception);
//
// The first parameter in this function is there only for the purpose of
// template type matching.
template<typename Exception, typename ObservedException>
Exception* ConvertException(Exception* /* dummy */,
                            ObservedException* observed) {
  if (observed == NULL) {
    return NULL;
  }
  scoped_ptr<ObservedException> observed_scoped(observed);
  return new Exception(*observed);
}

// Partial specialization so we do not do copy when exception is already of the
// same type.
template<typename Exception>
Exception* ConvertException(Exception* /* dummy */, Exception* observed) {
  return observed;
}

// Implementation note: to support the syntax shown at the top of the file, we
// rely on implicit conversion from 'propagator' classes to the FailureOr*
// classes. That conversion is performed via implicit constructors in the
// FailureOr* classes. For example, given this statement:
//
//   FailureOr<Cursor*> result = Success(new CoolCursor);
//
// The compiler will first instantiate and call:
//
//   ReferencePropagator<CoolCursor*> Success<CoolCursor*>(CoolCursor& result);
//
// And then materialize and invoke the following constructor:
//
//   FailureOr<CoolCursor*>::FailureOr(const ReferencePropagator<CoolCursor*>&)
//
// (Exception to the C++ Style Guide rule on implicit exceptions granted by
// csilvers; see http://groups/c-style/browse_thread/thread/5c18ccb21d72923e).

// For void results. Can be considered a quasi-specialization for
// FailureOr<void>. The Exception class must provide a public destructor.
//
// To create an instance of this class, call:
//     FailureOrVoid foo(Success()) or
//     FailureOrVoid foo(Failure(...)).

template<typename Exception>
class FailureOrVoid {
 public:
  template<typename ObservedException>
  FailureOrVoid(const FailurePropagator<ObservedException>& failure)   // NOLINT
      : exception_(failure.exception) {}

  FailureOrVoid(const VoidPropagator&) : exception_(NULL) {}           // NOLINT

  bool is_success() const {
    mark_checked();
    return !is_failure();
  }
  bool is_failure() const {
    mark_checked();
    return exception_.get() != NULL;
  }

  const Exception& exception() const {
    EnsureFailure();
    return *exception_.get();
  }

  inline void mark_checked() const {
#if SUPERSONIC_FORCE_FAILURE_CHECK
    checked_.set();
#endif
  }

  // If is_failure() is true, releases the associated exception object and
  // returns it to the caller. Otherwise, returns NULL.
  Exception* release_exception() {
    mark_checked();
    return exception_.release();
  }

 protected:
  void EnsureFailure() const {
    ensure_checked();
    CHECK(is_failure()) << "Expected failure, but hasn't found one.";
  }

  void EnsureSuccess() const {
    ensure_checked();
    CHECK(is_success()) << "Unexpected failure: "
        << exception().PrintStackTrace();
  }

 private:
  inline void ensure_checked() const {
#if SUPERSONIC_FORCE_FAILURE_CHECK
    DCHECK(checked_.get())
        << "FailureOr... object was not checked for success or failure.";
#endif
  }

  CoownedPointer<Exception> exception_;
#if SUPERSONIC_FORCE_FAILURE_CHECK
  // This field is used in debug mode to detect missing exception checks. Using
  // an additional boolean flat we can detect that a check wasn't performed even
  // if there was no exception.
  //
  // The way CheckedFlag works is that it is always reset to false in copies and
  // assignments - the assumption is that copies should be checked again for
  // errors (for example, when a function gets a FailureOr*<...> value from
  // another function that already checked for error, the outer function should
  // checks for errors again).
  mutable internal::CheckedFlag checked_;
#endif
  // Copyable.
};

// For results passed by value. The Result class must provide a default
// constructor (used when a failure is propagated). (If this is not acceptable,
// consider using FailureOrConstReference). The Exception class must
// provide a public destructor.
//
// To create an instance of this class, call:
//     FailureOr<Result> foo(Success(value)) or
//     FailureOr<Result> foo(Failure(...)).
template<typename Result, typename Exception>
class FailureOr : public FailureOrVoid<Exception> {
 public:
  template<typename ObservedException>
  FailureOr(const FailurePropagator<ObservedException>& failure)       // NOLINT
      : FailureOrVoid<Exception>(failure) {}

  template<typename ObservedResult>
  FailureOr(const ReferencePropagator<ObservedResult>& result)         // NOLINT
      : FailureOrVoid<Exception>(Success()),
        result_(result.result) {}

  template<typename ObservedResult>
  FailureOr(const ConstReferencePropagator<ObservedResult>& result)    // NOLINT
      : FailureOrVoid<Exception>(Success()),
        result_(result.result) {}

  const Result& get() const {
    FailureOrVoid<Exception>::EnsureSuccess();
    return result_;
  }

 private:
  Result result_;
  // Copyable.
};

// For results passed by reference. The Exception class must provide a
// public destructor.
// NOTE: if Success, the referenced object must be valid for as long as
// FailureOrReference instances are in use, just as if this was a plain
// reference.
//
// To create an instance of this class, call:
//     FailureOrReference<T> foo(Success(variable)) or
//     FailureOrReference<T> foo(Failure(...)).
template<typename Result, typename Exception>
class FailureOrReference : public FailureOrVoid<Exception> {
 public:
  template<typename ObservedException>
  FailureOrReference(
      const FailurePropagator<ObservedException>& failure)             // NOLINT
      : FailureOrVoid<Exception>(failure) {}

  template<typename ObservedResult>
  FailureOrReference(
      const ReferencePropagator<ObservedResult>& result)               // NOLINT
      : FailureOrVoid<Exception>(Success()),
        result_(&result.result) {}

  template<typename ObservedResult>
  FailureOrReference(
      const ConstReferencePropagator<ObservedResult>& result)          // NOLINT
      : FailureOrVoid<Exception>(Success()),
        result_(result.result) {}

  Result& get() const {
    FailureOrVoid<Exception>::EnsureSuccess();
    return *result_;
  }

 private:
  Result* result_;
  // Copyable.
};

// For results passed by pointer w/ ownership transferred to the caller.
// The result is owned by this wrapper object until explicitly released.
// Semantics resemble the linked_ptr<T>. The Result and Exception classes
// must provide public destructors.
// This class behavies as scoped_ptr, and fullfils scoped_ptr interface.
//
// To create an instance of this class, call:
//     FailureOrOwned<T> foo(Success(owned_ptr)) or
//     FailureOrOwned<T> foo(Failure(...)).
template<typename Result, typename Exception>
class FailureOrOwned : public FailureOrVoid<Exception> {
 public:
  template<typename ObservedException>
  FailureOrOwned(const FailurePropagator<ObservedException>& failure)  // NOLINT
      : FailureOrVoid<Exception>(failure) {}

  template<typename ObservedResult>
  FailureOrOwned(const ReferencePropagator<ObservedResult>& result)    // NOLINT
      : FailureOrVoid<Exception>(Success()),
        result_(result.result) {}

  template<typename ObservedResult>
  FailureOrOwned(
      const ConstReferencePropagator<ObservedResult>& result)          // NOLINT
      : FailureOrVoid<Exception>(Success()),
        result_(result.result) {}

  // Returns a pointer to the result, without transferring ownership.
  const Result& operator*() const {
    FailureOrVoid<Exception>::EnsureSuccess();
    return *result_.get();
  }

  Result& operator*() {
    FailureOrVoid<Exception>::EnsureSuccess();
    return *result_.get();
  }

  Result* operator->() const {
    FailureOrVoid<Exception>::EnsureSuccess();
    return result_.get();
  }

  Result* operator->() {
    FailureOrVoid<Exception>::EnsureSuccess();
    return result_.get();
  }

  const Result* get() const {
    FailureOrVoid<Exception>::EnsureSuccess();
    return result_.get();
  }

  Result* get() {
    FailureOrVoid<Exception>::EnsureSuccess();
    return result_.get();
  }

  // Comparison operators.
  // These return whether a OwnedFailureOr and a raw pointer refer to
  // the same object, not just to two different but equal objects.
  // Works only for successful OwnedFailureOrs.
  bool operator==(const Result* p) const {
    FailureOrVoid<Exception>::EnsureSuccess();
    return result_.get() == p;
  }

  bool operator!=(const Result* p) const {
    FailureOrVoid<Exception>::EnsureSuccess();
    return result_.get() != p;
  }

  // Returns the enclosed result, passing the ownership to the caller.
  // Can be called only once on this FailureOrOwned object or any of its copies.
  // (Subsequent calls will cause crash).
  Result* release() {
    FailureOrVoid<Exception>::EnsureSuccess();
    return result_.release();
  }

 private:
  // Forbid comparison of FailureOrOwned types.  If Result != Result2, it
  // totally doesn't make sense, and if Result == Result2, it still doesn't make
  // sense because you should never have the same object owned by two different
  // FailureOrOwned<...>.
  template <class Result2, class Exception2> bool operator==(
      FailureOrOwned<Result2, Exception2> const& p2) const;
  template <class Result2, class Exception2> bool operator!=(
      FailureOrOwned<Result2, Exception2> const& p2) const;

  CoownedPointer<Result> result_;
  // Copyable.
};

// Adapters to turn exceptions into runtime crashes. They require that the
// exception class (the template parameter) supports PrintStackTrace() that
// can be streamed to the error log.
// Example usage:
//
// FailureOrReference<const Foo> FooFun();
// ...
// const Foo& foo = SucceedOrDie(FooFun());

template<typename Exception>
inline void SucceedOrDie(FailureOrVoid<Exception> result) {
  CHECK(result.is_success()) << result.exception().PrintStackTrace();
}

template<typename Result, typename Exception>
Result SucceedOrDie(FailureOr<Result, Exception> result) {
  CHECK(result.is_success()) << result.exception().PrintStackTrace();
  return result.get();
}

template<typename Result, typename Exception>
Result& SucceedOrDie(FailureOrReference<Result, Exception> result) {
  CHECK(result.is_success()) << result.exception().PrintStackTrace();
  return result.get();
}

// The ownership is passed to the caller.
template<typename Result, typename Exception>
Result* SucceedOrDie(FailureOrOwned<Result, Exception> result) {
  CHECK(result.is_success()) << result.exception().PrintStackTrace();
  return result.release();
}


// Implementation details below.

// Propagator classes.

template<typename Exception>
struct FailurePropagator {
  explicit FailurePropagator(Exception* exception) : exception(exception) {}
  Exception* exception;
};

template<typename Result>
struct ReferencePropagator {
  explicit ReferencePropagator(Result& result) : result(result) {}  // NOLINT
  Result& result;
};

template<typename Result>
struct ConstReferencePropagator {
  explicit ConstReferencePropagator(const Result& result) : result(result) {}
  const Result& result;
};

// Success & Failure implementations.

template<typename Exception>
FailurePropagator<Exception> Failure(Exception* exception) {
  return FailurePropagator<Exception>(exception);
}

inline VoidPropagator Success() { return VoidPropagator(); }

template<typename Result>
ReferencePropagator<Result> Success(Result& result) {  // NOLINT
  return ReferencePropagator<Result>(result);
}

template<typename Result>
ConstReferencePropagator<Result> Success(const Result& result) {
  return ConstReferencePropagator<Result>(result);
}

}  // namespace common

#endif  // DATAWAREHOUSE_COMMON_EXCEPTION_FAILUREOR_H_
