#include <core/app-template.hh>
#include <core/reactor.hh>
#include <core/print.hh>
#include <chrono>

using namespace seastar;
using namespace std::chrono_literals;

#define BUG() do { \
        std::cerr << "ERROR @ " << __FILE__ << ":" << __LINE__ << std::endl; \
        throw std::runtime_error("test failed"); \
    } while (0)

#define OK() do { \
        std::cerr << "OK @ " << __FILE__ << ":" << __LINE__ << std::endl; \
    } while (0)

template <typename Clock>
struct timer_test {
    timer<Clock> t1;
    timer<Clock> t2;
    timer<Clock> t3;
    timer<Clock> t4;
    timer<Clock> t5;
    promise<> pr1;
    promise<> pr2;

    future<> run() {
        t1.set_callback([this] {
            OK();
            print(" 500ms timer expired\n");
            if (!t4.cancel()) {
                BUG();
            }
            if (!t5.cancel()) {
                BUG();
            }
            t5.arm(1100ms);
        });
        t2.set_callback([] { OK(); print(" 900ms timer expired\n"); });
        t3.set_callback([] { OK(); print("1000ms timer expired\n"); });
        t4.set_callback([] { OK(); print("  BAD cancelled timer expired\n"); });
        t5.set_callback([this] { OK(); print("1600ms rearmed timer expired\n"); pr1.set_value(); });

        t1.arm(500ms);
        t2.arm(900ms);
        t3.arm(1000ms);
        t4.arm(700ms);
        t5.arm(800ms);

        return pr1.get_future().then([this] { return test_timer_cancelling(); });
    }

    future<> test_timer_cancelling() {
        timer<Clock>& t1 = *new timer<Clock>();
        t1.set_callback([] { BUG(); });
        t1.arm(100ms);
        t1.cancel();

        t1.arm(100ms);
        t1.cancel();

        t1.set_callback([this] { OK(); pr2.set_value(); });
        t1.arm(100ms);
        return pr2.get_future().then([&t1] { delete &t1; });
    }
};

int main(int ac, char** av) {
    app_template app;
    timer_test<steady_clock_type> t1;
    timer_test<lowres_clock> t2;
    return app.run(ac, av, [&t1, &t2] () -> future<int> {
        print("=== Start High res clock test\n");
        return t1.run().then([&t2] {
            print("=== Start Low  res clock test\n");
            return t2.run();
        }).then([] {
            print("Done\n");
            return make_ready_future<int>(0);
        });
    });
}
