// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include <chrono>
#include <core/app-template.hh>
#include <core/reactor.hh>
#include <core/print.hh>
#include <core/sleep.hh>
#include <core/distributed.hh>

using namespace seastar;
using namespace std::chrono_literals;
namespace bpo = boost::program_options;
using std::string;

class Mr {

 public:
  Mr() {
    print("cpuid: %d\n", engine().cpu_id());
  }
  future<> Process(sstring item) {
    print("file: %s\n", item);
    return make_ready_future<>();
  }
  future<> stop() { return make_ready_future<>(); }
};

class DistributedMr final : public distributed<Mr> {
  unsigned next_ = 0;

 public:
  future<> invoke(sstring fname) {
    unsigned id = next_;
    next_ = (next_ + 1) % smp::count;
    print("invoke on : %d\n", id);

    return invoke_on(id, &Mr::Process, std::move(fname));
  }
};

future<> ListDir(file dir, DistributedMr* dmr) {
  printf("Before listing\n");

  auto listing = make_lw_shared<subscription<directory_entry>>(
      dir.list_directory([dmr](directory_entry de) {
       return dmr->invoke(de.name);
    }));

  return listing->done()
    .finally([listing, dir = std::move(dir)] {});
}

future<int> AppRun(const bpo::variables_map& config) {
  sstring dir = config["dir"].as<std::string>();
  print("%s\n", dir);

  return engine().open_directory(dir).then_wrapped([](future<file>&& ff) {
    try {
      file dir = ff.get0();

      auto mr = make_lw_shared<DistributedMr>();

      return mr->start().then(
          [ mr, dir = std::move(dir)] {
            return ListDir(dir, mr.get());
          })
        .then([mr] { return mr->stop(); })
        .then([] { return 0; });
    } catch (std::exception& e) {
      std::cout << "exception2: " << e << "\n";

      return make_ready_future<int>(-1);
    }
  });
}

int main(int ac, char** av) {
    app_template app;

    app.add_options()
                ("dir", bpo::value<std::string>()->required(), "directory")
                ;

    return app.run(ac, av, [&app] {
        print("=== Start High res clock test\n");
        return AppRun(app.configuration());
    });
}
