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
  }
  future<> Process(sstring& item) {
    print("file: %s\n", item);
    return make_ready_future<>();
  }
  future<> stop() { return make_ready_future<>(); }
};

class DistributedMr final : public distributed<Mr> {
  unsigned next_id_ = 0;
  file dir_;

  std::experimental::optional<subscription<directory_entry>> listing_;

  future<> invoke(sstring fname) {
    unsigned id = next_id_;
    next_id_ = (next_id_ + 1) % smp::count;

    return do_with(std::move(fname), [this, id](sstring& fname) {
      return invoke_on(id, &Mr::Process, std::ref(fname));
    });
  }

 public:
  future<> register_for_dir(file dir) {
    dir_ = std::move(dir);

    listing_.emplace(dir_.list_directory([this](directory_entry de) {
       return invoke(de.name);
    }));
    return listing_->done();
  }


  ~DistributedMr() {
  }
};

future<int> AppRun(const bpo::variables_map& config) {
  sstring dir = config["dir"].as<std::string>();
  print("%s\n", dir);

  return engine().open_directory(dir).then_wrapped([](future<file>&& ff) {
    try {
      file dir = ff.get0();

      auto mr = make_lw_shared<DistributedMr>();

      auto res = mr->start().then(
        [ mr, dir = std::move(dir)] {
            return mr->register_for_dir(dir);
          })
        .then([mr] {
          return mr->stop(); })
        .then([mr] { return 0; });
      return res;
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
        print("=== MR test\n");
        return AppRun(app.configuration());
    });
}
