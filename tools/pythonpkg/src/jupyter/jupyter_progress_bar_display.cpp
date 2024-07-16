#include "duckdb_python/jupyter_progress_bar_display.hpp"
#include "duckdb_python/pyconnection/pyconnection.hpp"
#include "duckdb_python/nanobind/nb_wrapper.hpp"

namespace duckdb {

unique_ptr<ProgressBarDisplay> JupyterProgressBarDisplay::Create() {
	return make_uniq<JupyterProgressBarDisplay>();
}

void JupyterProgressBarDisplay::Initialize() {
	auto &import_cache = *DuckDBPyConnection::ImportCache();
	auto float_progress_attr = import_cache.ipywidgets.FloatProgress();
	D_ASSERT(float_progress_attr.ptr() != nullptr);
	// Initialize the progress bar
	nb::dict style;
	style["bar_color"] = "black";
	progress_bar = float_progress_attr((nb::arg("min") = 0, nb::arg("max") = 100, nb::arg("style") = style));

	progress_bar.attr("layout").attr("width") = "auto";

	// Display the progress bar
	auto display_attr = import_cache.IPython.display.display();
	D_ASSERT(display_attr.ptr() != nullptr);
	display_attr(progress_bar);
}

JupyterProgressBarDisplay::JupyterProgressBarDisplay() : ProgressBarDisplay() {
	// Empty, we need the GIL to initialize, which we don't have here
}

void JupyterProgressBarDisplay::Update(double progress) {
	nb::gil_scoped_acquire gil;
	if (progress_bar.ptr() == nullptr) {
		// First print, we first need to initialize the display
		Initialize();
	}
	progress_bar.attr("value") = nb::cast(progress);
}

void JupyterProgressBarDisplay::Finish() {
	Update(100);
}

} // namespace duckdb
