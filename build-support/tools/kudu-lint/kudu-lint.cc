// Copyright (c) 2013, Cloudera, inc.
//
// Based somewhat on the example from 

#include "clang/AST/ASTContext.h"
#include "clang/AST/ASTTypeTraits.h"
#include "clang/ASTMatchers/ASTMatchers.h"
#include "clang/ASTMatchers/ASTMatchFinder.h"
#include "clang/AST/Stmt.h"
#include "clang/Driver/Options.h"
#include "clang/Frontend/FrontendActions.h"
#include "clang/Frontend/TextDiagnostic.h"
#include "clang/Tooling/CommonOptionsParser.h"
#include "clang/Tooling/Tooling.h"
#include "llvm/Option/OptTable.h"
#include "llvm/Support/CommandLine.h"

// Using clang without importing namespaces is damn near impossible.
using namespace llvm; // NOLINT
using namespace clang::ast_matchers; // NOLINT

using llvm::opt::OptTable;
using clang::ast_type_traits::DynTypedNode;
using clang::driver::createDriverOptTable;
using clang::driver::options::OPT_ast_dump;
using clang::tooling::CommonOptionsParser;
using clang::tooling::CommandLineArguments;
using clang::tooling::ClangTool;
using clang::tooling::newFrontendActionFactory;
using clang::ASTContext;
using clang::CharSourceRange;
using clang::ClassTemplateSpecializationDecl;
using clang::Decl;
using clang::DiagnosticsEngine;
using clang::Expr;
using clang::FixItHint;
using clang::SourceLocation;
using clang::SourceRange;
using clang::Stmt;
using clang::TextDiagnostic;

static cl::extrahelp CommonHelp(CommonOptionsParser::HelpMessage);
static cl::extrahelp MoreHelp(
    "\tFor example, to run kudu-lint on all files in a subtree of the\n"
    "\tsource tree, use:\n"
    "\n"
    "\t  find path/in/subtree -name '*.cc'|xargs kudu-lint\n"
    "\n"
    "\tor using a specific build path:\n"
    "\n"
    "\t  find path/in/subtree -name '*.cc'|xargs kudu-lint -p build/path\n"
    "\n"
    "\tNote, that path/in/subtree and current directory should follow the\n"
    "\trules described above.\n"
    "\n"
    "\t Sometimes kudu-lint can't figure out the proper line number for an\n"
    "\t error, and reports it inside some standard library. the -ast-dump\n"
    "\t option can be useful for these circumstances.\n"
    "\n"
);

// Command line flags.
static OwningPtr<OptTable> gOptions(createDriverOptTable());
static cl::list<std::string> gArgsAfter(
  "extra-arg",
  cl::desc("Additional argument to append to the compiler command line"));
static cl::list<std::string> gArgsBefore(
  "extra-arg-before",
  cl::desc("Additional argument to prepend to the compiler command line"));
static cl::opt<bool> gASTDump("ast-dump",
                              cl::desc(gOptions->getOptionHelpText(OPT_ast_dump)));

namespace {

// Callback for unused statuses. Simply reports the error at the point where the
// expression was found.
class UnusedStatusMatcher : public MatchFinder::MatchCallback {
 public:
  virtual void run(const MatchFinder::MatchResult& result) {
    const Expr* expr;
    if ((expr = result.Nodes.getNodeAs<Expr>("expr"))) {
      if (gASTDump) {
        expr->dump();
      }

      SourceRange r = expr->getSourceRange();
      if (r.isValid()) {
        TextDiagnostic td(llvm::outs(), result.Context->getLangOpts(),
                          &result.Context->getDiagnostics().getDiagnosticOptions());
        td.emitDiagnostic(r.getBegin(), DiagnosticsEngine::Error,
                          "Found unused Status",
                          ArrayRef<CharSourceRange>(CharSourceRange::getTokenRange(r)),
                          ArrayRef<FixItHint>(), result.SourceManager);
      }

      SourceLocation instantiation_point;
      if (FindInstantiationPoint(result, expr, &instantiation_point)) {
        TextDiagnostic td(llvm::outs(), result.Context->getLangOpts(),
                          &result.Context->getDiagnostics().getDiagnosticOptions());
        td.emitDiagnostic(instantiation_point, DiagnosticsEngine::Note,
                          "previous error instantiated at",
                          ArrayRef<CharSourceRange>(),
                          ArrayRef<FixItHint>(), result.SourceManager);
      }
    } else {
      llvm_unreachable("bound node missing");
    }
  }

 private:
  bool GetParent(ASTContext* ctx, const DynTypedNode& node, DynTypedNode* parent) {
    ASTContext::ParentVector parents = ctx->getParents(node);
    if (parents.empty()) return false;
    assert(parents.size() == 1);
    *parent = parents[0];
    return true;
  }

  // If the AST node 'expr' has an ancestor which is a template instantiation,
  // fill the source location of that instantiation into 'loc'. Unfortunately,
  // Clang doesn't retain enough information in the AST nodes to recurse here --
  // so in many cases this is useless, since the instantiation point will simply
  // be inside another instantiated template.
  bool FindInstantiationPoint(const MatchFinder::MatchResult& result, const Expr* expr,
                              SourceLocation* loc) {
    DynTypedNode node = DynTypedNode::create<Expr>(*expr);

    // Recurse up the tree.
    while (true) {
      if (const ClassTemplateSpecializationDecl* D = node.get<ClassTemplateSpecializationDecl>()) {
        *loc = D->getPointOfInstantiation();
        return true;
      }
      // TODO: there are probably other types of specializations to handle, but this is the only
      // one seen so far.

      DynTypedNode parent;
      if (!GetParent(result.Context, node, &parent)) {
        return false;
      }
      node = parent;
    }
  }
};

// Inserts arguments before or after the usual command line arguments.
class InsertAdjuster: public clang::tooling::ArgumentsAdjuster {
 public:
  enum Position { BEGIN, END };

  InsertAdjuster(const CommandLineArguments &extra_, Position pos)
    : extra_(extra_), pos_(pos) {
  }

  InsertAdjuster(const char *extra_, Position pos)
    : extra_(1, std::string(extra_)), pos_(pos) {
  }

  virtual CommandLineArguments Adjust(const CommandLineArguments &Args) LLVM_OVERRIDE {
    CommandLineArguments ret(Args);

    CommandLineArguments::iterator I;
    if (pos_ == END) {
      I = ret.end();
    } else {
      I = ret.begin();
      ++I; // To leave the program name in place
    }

    ret.insert(I, extra_.begin(), extra_.end());
    return ret;
  }

 private:
  const CommandLineArguments extra_;
  const Position pos_;
};

} // anonymous namespace

int main(int argc, const char **argv) {
  CommonOptionsParser options_parser(argc, argv);
  ClangTool Tool(options_parser.getCompilations(),
                 options_parser.getSourcePathList());
  if (gArgsAfter.size() > 0) {
    Tool.appendArgumentsAdjuster(new InsertAdjuster(gArgsAfter,
          InsertAdjuster::END));
  }
  if (gArgsBefore.size() > 0) {
    Tool.appendArgumentsAdjuster(new InsertAdjuster(gArgsBefore,
          InsertAdjuster::BEGIN));
  }

  // Match expressions of type 'Status' which are parented by a compound statement.
  // This implies that the expression is being thrown away, rather than assigned
  // to some variable or function call.
  //
  // For more information on AST matchers, refer to:
  // http://clang.llvm.org/docs/LibASTMatchersReference.html
  StatementMatcher ignoredStatusMatcher =
    expr(hasType(recordDecl(hasName("Status"))),
         hasParent(compoundStmt())).bind("expr");
  MatchFinder finder;
  UnusedStatusMatcher printer;
  finder.addMatcher(ignoredStatusMatcher, &printer);
  return Tool.run(newFrontendActionFactory(&finder));
}
