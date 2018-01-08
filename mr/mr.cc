// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include <chrono>
#include <core/app-template.hh>
#include <core/do_with.hh>
#include <core/fstream.hh>
#include <core/reactor.hh>
#include <core/print.hh>
#include <core/sleep.hh>
#include <core/distributed.hh>

using namespace seastar;
using namespace std::chrono_literals;
namespace bpo = boost::program_options;
using std::string;


future<> read_file_ex(file f, sstring fname) {
  file* fptr = new file(std::move(f));

  return fptr->dma_read<char>(0, 1 << 13).then([fptr] (temporary_buffer<char>) {
     return fptr->close();
   }).finally([guard = std::unique_ptr<file>(fptr)] {});

  /* auto is = make_lw_shared<input_stream<char>>(make_file_input_stream(std::move(f)));

  return is->read().then([is] (temporary_buffer<char> buf) {
      return is->close().finally([is] {});*/
  // });
}

/*
  struct StreamConsumer {
    using consumption_result_type = typename input_stream<char>::consumption_result_type;

    explicit StreamConsumer(Mr* mr) : mr_(mr) {}

   public:
    future<consumption_result_type> operator()(temporary_buffer<char> buf) {
      using stop_consuming_type = typename consumption_result_type::stop_consuming_type;
      return make_ready_future<consumption_result_type>(stop_consuming_type({}));
    }
   private:
    Mr* mr_;
  };

*/

class Mr {
  sstring dir_;

 public:


  Mr(sstring dir) : dir_(dir) {}

  future<> process(sstring& fname) {
    sstring full_name = dir_ + "/" + fname;
    print("name %s\n", fname);

    return open_file_dma(full_name, open_flags::ro)
      .then([full_name] (file f) { return read_file_ex(std::move(f), full_name); })

          /*return is.consume(StreamConsumer{this})
            .finally([&is] {

             print("finally\n");
             return is.close();
           });*/
      .handle_exception([full_name] (std::exception_ptr eptr) {
        std::cerr << "Error: " << eptr << " for " << full_name << std::endl;
      });
  }

  future<> stop() { return make_ready_future<>(); }


 private:

};

class DistributedMr final : public distributed<Mr> {
  unsigned next_id_ = 0;
  file dir_fd_;
  std::experimental::optional<subscription<directory_entry>> listing_;

  future<> invoke(sstring fname) {
    unsigned id = next_id_;
    next_id_ = (next_id_ + 1) % smp::count;

    return do_with(std::move(fname), [this, id](sstring& fname) {
      //return local().process(fname);
      return invoke_on(id, &Mr::process, std::ref(fname));
    });
  }

 public:
  DistributedMr() {}

  future<> register_for_dir(file dir_fd) {
    dir_fd_ = std::move(dir_fd);

    listing_.emplace(dir_fd_.list_directory([this](directory_entry de) {
      if (de.type == directory_entry_type::regular)
        return invoke(de.name);
      else
        return make_ready_future<>();
    }));
    return listing_->done();
  }

  ~DistributedMr() {
  }
};

future<int> AppRun(const bpo::variables_map& config) {
  sstring dir = config["dir"].as<std::string>();
  print("%s\n", dir);

  return engine().open_directory(dir).then_wrapped([dir](future<file>&& ff) {
    try {
      file dir_fd = ff.get0();

      auto mr = make_lw_shared<DistributedMr>();

      auto res = mr->start(dir).then(
        [ mr, dir_fd = std::move(dir_fd)] {
            return mr->register_for_dir(dir_fd);
          })
        .then([mr] {
          return mr->stop(); })
        .then([mr] { return 0; });
      return res;
    } catch (std::exception& e) {
      std::cout << "exception: " << e << "\n";

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
