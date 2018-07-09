#include "logger.hpp"

#include <algorithm>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sstream>
#include <boost/filesystem.hpp>
#include <boost/range.hpp>
#include <boost/range/algorithm/reverse.hpp>

#include "abstract_logger.hpp"
#include "group_commit_logger.hpp"
#include "simple_logger.hpp"
#include "no_logger.hpp"

namespace opossum {

// set default Implementation
const Logger::Implementation Logger::default_implementation = Implementation::GroupCommit;

// Logging is initially set to NoLogger for setup
Logger::Implementation Logger::_implementation = Implementation::No;

const std::string Logger::default_data_path = "./data/";

std::string Logger::data_path = default_data_path;
const std::string Logger::log_folder = "logs/";
std::string Logger::log_path = data_path + log_folder;
const std::string Logger::filename = "hyrise-log";

AbstractLogger& Logger::getInstance() {
  switch (_implementation) {
    case Implementation::No: { static NoLogger instance; return instance; }
    case Implementation::Simple: { static SimpleLogger instance; return instance; }
    case Implementation::GroupCommit: { static GroupCommitLogger instance; return instance; }
  }
}

// This function should only be called by tests
void Logger::_shutdown_after_all_tests() {
  for (auto impl : {Implementation::Simple, Implementation::GroupCommit}) {
    _implementation = impl;
    _reconstruct();
  }
}

// This function should only be called by tests
void Logger::_set_implementation(const Logger::Implementation implementation) {
  getInstance()._shut_down();
  switch (_implementation) {
    case Implementation::No: { break; }
    case Implementation::Simple: { static_cast<SimpleLogger&>(getInstance()).~SimpleLogger(); break; }
    case Implementation::GroupCommit: { static_cast<GroupCommitLogger&>(getInstance()).~GroupCommitLogger(); break; }
  }
  _implementation = implementation;
}

// This function should only be called by tests
void Logger::_reconstruct() {
  switch (_implementation) {
    case Implementation::No: { break; }
    case Implementation::Simple: { new(&getInstance()) SimpleLogger(); break; }
    case Implementation::GroupCommit: { new(&getInstance()) GroupCommitLogger(); break; }
  }
}

void Logger::setup(std::string folder, const Implementation implementation) {
  DebugAssert(_implementation == Implementation::No, "Logger: changing folder but may have open file handle.");
  DebugAssert(folder.length() > 0, "Logger: empty string is no folder");
  DebugAssert(folder[folder.size() - 1] == '/', "Logger: expected '/' at end of path");
  
  data_path = folder;
  log_path = data_path + log_folder;

  _create_directories();

  _set_implementation(implementation);
}

void Logger::delete_log_files() {
  boost::filesystem::remove_all(log_path);
  _create_directories();
}

void Logger::_create_directories() {
  boost::filesystem::create_directory(data_path);
  boost::filesystem::create_directory(log_path);
}

std::string Logger::get_new_log_path() {
  auto log_number = _get_latest_log_number() + 1;
  std::string path = log_path + filename + std::to_string(log_number);
  return path;
}

std::vector<std::string> Logger::get_all_log_file_paths() {
  DebugAssert(boost::filesystem::exists(log_path), "Logger: Log path does not exist.");
  std::vector<std::string> result;
  for (auto& path : boost::make_iterator_range(boost::filesystem::directory_iterator(log_path), {})) {
    auto pos = path.path().string().rfind(filename);
    if (pos == std::string::npos) {
      continue;
    }
    result.push_back(path.path().string());
  }

  if (result.size() > 0) {
    auto pos = result[0].rfind(filename) + filename.length();
    std::sort(result.begin(), result.end(), [&pos](std::string a, std::string b){ 
      return std::stoul(a.substr(pos)) < std::stoul(b.substr(pos)); });
  }
  return (result);
}

u_int32_t Logger::_get_latest_log_number() {
  u_int32_t max_number{0};

  for (auto& path : boost::make_iterator_range(boost::filesystem::directory_iterator(log_path), {})) {
    auto pos = path.path().string().rfind(filename);
    if (pos == std::string::npos) {
      continue;
    }

    u_int32_t number = std::stoul(path.path().string().substr(pos + filename.length()));
    max_number = std::max(max_number, number);
  }
  return max_number;
}

}  // namespace opossum
