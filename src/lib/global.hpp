
namespace opossum {

struct Global {
  static Global& get();

  const bool jit = false;
  const bool lazy_load = false;
  const bool jit_validate = false;
};

}  // namespace opossum
