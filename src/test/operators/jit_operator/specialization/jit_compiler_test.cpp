#include <llvm/IRReader/IRReader.h>
#include <llvm/Support/SourceMgr.h>
#include <llvm/Support/TargetSelect.h>

#include "../../../base_test.hpp"
#include "operators/jit_operator/specialization/jit_compiler.hpp"
#include "operators/jit_operator/specialization/llvm_utils.hpp"

namespace opossum {

extern char jit_compiler_test_module;
extern size_t jit_compiler_test_module_size;

class JitCompilerTest : public BaseTest {
 protected:
  void SetUp() override {
    // Global LLVM initializations
    llvm::InitializeNativeTarget();
    llvm::InitializeNativeTargetAsmParser();
    llvm::InitializeNativeTargetAsmPrinter();

    _context = std::make_shared<llvm::LLVMContext>();
    _module = parse_llvm_module(std::string(&jit_compiler_test_module, jit_compiler_test_module_size), *_context);
  }

  std::shared_ptr<llvm::LLVMContext> _context;
  std::unique_ptr<llvm::Module> _module;
  std::string _add_fn_symbol = "_Z3addii";
};

TEST_F(JitCompilerTest, CompilesAddedModules) {
  auto compiler = JitCompiler(_context);
  compiler.add_module(std::move(_module));
  auto add_fn = compiler.find_symbol<int32_t(int32_t, int32_t)>(_add_fn_symbol);
  ASSERT_EQ(add_fn(1, 5), 1 + 5);
}

TEST_F(JitCompilerTest, ThrowsOnInvalidSymbolLookup) {
  auto compiler = JitCompiler(_context);
  compiler.add_module(std::move(_module));
  ASSERT_THROW(compiler.find_symbol<void()>("some_nonexisting_symbol"), std::logic_error);
}

TEST_F(JitCompilerTest, AddsAndRemovesModules) {
  auto compiler = JitCompiler(_context);
  ASSERT_THROW(compiler.find_symbol<int32_t(int32_t, int32_t)>(_add_fn_symbol), std::logic_error);
  auto module_handle = compiler.add_module(std::move(_module));
  compiler.find_symbol<int32_t(int32_t, int32_t)>(_add_fn_symbol);
  compiler.remove_module(module_handle);
  ASSERT_THROW(compiler.find_symbol<int32_t(int32_t, int32_t)>(_add_fn_symbol), std::logic_error);
}

}  // namespace opossum
