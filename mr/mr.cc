// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include <chrono>
#include <core/app-template.hh>
#include <core/do_with.hh>
#include <core/fstream.hh>
#include <core/gate.hh>
#include <core/reactor.hh>
#include <core/print.hh>
#include <core/sleep.hh>
#include <core/distributed.hh>

using namespace seastar;
using namespace std::chrono_literals;
namespace bpo = boost::program_options;
using std::string;

bool stop_and_break = false;

template<typename Container> temporary_buffer<char>
  Flatten(const Container& c,
          temporary_buffer<char> tail = temporary_buffer<char>()) {
  if (c.empty())
    return tail;

  using tb_t = temporary_buffer<char>;
  size_t total = std::accumulate(c.begin(), c.end(), 0,
                                 [](size_t val, const tb_t& tb) { return val + tb.size();});
  total += tail.size();
  tb_t res(total);
  char* dest = res.get_write();
  for (const auto& src : c) {
    dest = std::copy(src.begin(), src.end(), dest);
  }
  if (!tail.empty())
    std::copy(tail.begin(), tail.end(), dest);

  return res;
}

template<typename T> future<std::decay_t<T>> make_value_future(T&& val) {
  return make_ready_future<std::decay_t<T>>(std::forward<T>(val));
}

subscription<temporary_buffer<char>>
emit_lines(input_stream<char> is, subscription<temporary_buffer<char>>::next_fn fn) {
  using subscr_t = subscription<temporary_buffer<char>>;
  using tmp_buf_t = temporary_buffer<char>;

  class work {
    std::vector<tmp_buf_t> pending_;
    input_stream<char> is_;
    stream<temporary_buffer<char>> producer_;
    unsigned lines_ = 0;

   public:
    work(const work&) = delete;

    work(input_stream<char> is) : is_(std::move(is)) {}

    subscr_t listen(subscr_t::next_fn fn) {
      return producer_.listen(std::move(fn));
    }

    future<> emit_lines() {
      return producer_.started().then([this] {
        return repeat([this] () {
          return is_.read().then([this] (tmp_buf_t tmp) {
            if (tmp.empty()) {
              if (!pending_.empty()) {
                tmp_buf_t line = Flatten(pending_);
                pending_.clear();

                return producer_.produce(std::move(line)).then([] {
                  return make_value_future(stop_iteration::yes);
                });
              }
              return make_value_future(stop_iteration::yes);
            }

            return split_lines(std::move(tmp)).then([] () { return stop_iteration::no; });
          });
      }).then([this] {
          producer_.close();
        }).handle_exception([this] (std::exception_ptr e) {
          producer_.set_exception(e);
        });
      }).then([this] {
        return is_.close();
    });
   }

   private:
    future<> split_lines(tmp_buf_t block) {
      assert(!block.empty());

      auto it = std::find(block.begin(), block.end(), '\n');
      if (it == block.end()) {
        pending_.push_back(std::move(block));
        return make_ready_future<>();
      }

      size_t sz = 1 + (it - block.begin());
      tmp_buf_t line = Flatten(pending_, block.share(0, sz));
      pending_.clear();
      block.trim_front(sz);

      ++lines_;
      future<> f = producer_.produce(std::move(line));

      if (block.empty()) {
        return f;
      }

      return f.then([this, block = std::move(block)] () mutable {
                return split_lines(std::move(block)); }
             );
    }

  };

  work* w = new work(std::move(is));
  subscr_t res = w->listen(std::move(fn));
  w->emit_lines()  // never throws due to handle_exception inside.
    .then([w] { delete w;});

  return res;
}

class Mr {
  sstring dir_;

  typedef temporary_buffer<char> tmp_buf_t;
  unsigned lines_ = 0;

  gate g_;
  semaphore open_limit_{10};

  future<> process_file(sstring fname) {
    return open_file_dma(fname, open_flags::ro)
      .then([this] (file f) {

        file_input_stream_options fo;
        fo.buffer_size = 1 << 14;
        fo.read_ahead = 1;

        subscription<tmp_buf_t> subscr =
            emit_lines(make_file_input_stream(std::move(f), fo),
                       [this] (tmp_buf_t line) {
                          return EmitSingleLine(std::move(line));
                        });

        return do_with(std::move(subscr), [this] (auto& subscr) {
          return subscr.done().then([this] {
              print("Counted %d lines\n", lines_);
              lines_ = 0;
            });
          }
        );  // do_with
      }) // open_file_dma
      .handle_exception([fname] (std::exception_ptr eptr) {
        std::cerr << "Error: " << eptr << " for " << fname << std::endl;
      });
  };

 public:
  Mr(sstring dir) : dir_(dir) {}

  future<> EmitSingleLine(tmp_buf_t line) {
    ++lines_;

    return make_ready_future<>();
  }

  future<> process(sstring& fname) {
    g_.enter();
    sstring full_name = dir_ + "/" + fname;
    return open_limit_.wait().then([this, full_name = std::move(full_name)] {
        if (stop_and_break)
          return make_ready_future<>();
        print("name %s %d\n", full_name, engine().cpu_id());
        return process_file(full_name);
      }).then([this] {
        open_limit_.signal();
        g_.leave();
      });
  }

  future<> stop() { return g_.close(); }

 private:
};

class DistributedMr final : public distributed<Mr> {
  unsigned next_id_ = 0;
  file dir_fd_;
  std::experimental::optional<subscription<directory_entry>> listing_;

  future<> invoke(sstring fname) {
    unsigned id = next_id_;
    next_id_ = (next_id_ + 1) % smp::count;

    do_with(std::move(fname), [this, id](sstring& fname) {
      return invoke_on(id, &Mr::process, std::ref(fname));
    }).then_wrapped([] (future<>&& f) {
      f.ignore_ready_future();
    });

    return make_ready_future<>();
  }

 public:
  DistributedMr() {}

  future<> list_dir(file dir_fd) {
    dir_fd_ = std::move(dir_fd);

    listing_.emplace(dir_fd_.list_directory([this](directory_entry de) {
      if (!stop_and_break && de.type == directory_entry_type::regular)
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

  engine().at_exit([] {
    stop_and_break = true;
    return make_ready_future<>();
  });

  return engine().open_directory(dir).then_wrapped([dir](future<file>&& ff) {
    try {
      file dir_fd = ff.get0();

      auto mr = make_lw_shared<DistributedMr>();

      auto res = mr->start(dir).then(
        [ mr, dir_fd = std::move(dir_fd)] {
            return mr->list_dir(dir_fd);
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
