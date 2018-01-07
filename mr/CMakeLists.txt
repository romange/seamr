add_executable(mr_demo mr.cc)
cxx_link(mr_demo TRDP::seastar)
