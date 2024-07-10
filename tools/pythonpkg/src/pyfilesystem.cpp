#include "duckdb_python/pyfilesystem.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb_python/nanobind/nb_wrapper.hpp"
#include "duckdb_python/nanobind/gil_wrapper.hpp"

namespace duckdb {

PythonFileHandle::PythonFileHandle(FileSystem &file_system, const string &path, const nb::object &handle)
    : FileHandle(file_system, path), handle(handle) {
}
PythonFileHandle::~PythonFileHandle() {
	try {
		PythonGILWrapper gil;
		handle.dec_ref();
		handle.release();
	} catch (...) { // NOLINT
	}
}

PythonFilesystem::~PythonFilesystem() {
	try {
		PythonGILWrapper gil;
		filesystem.dec_ref();
		filesystem.release();
	} catch (...) { // NOLINT
	}
}

string PythonFilesystem::DecodeFlags(FileOpenFlags flags) {
	// see https://stackoverflow.com/a/58925279 for truth table of python file modes
	bool read = flags.OpenForReading();
	bool write = flags.OpenForWriting();
	bool append = flags.OpenForAppending();
	bool truncate = flags.OverwriteExistingFile();

	string flags_s;
	if (read && write && truncate) {
		flags_s = "w+";
	} else if (read && write && append) {
		flags_s = "a+";
	} else if (read && write) {
		flags_s = "r+";
	} else if (read) {
		flags_s = "r";
	} else if (write) {
		flags_s = "w";
	} else if (append) {
		flags_s = "a";
	} else {
		throw InvalidInputException("%s: unsupported file flags", GetName());
	}

	flags_s.insert(1, "b"); // always read in binary mode

	return flags_s;
}

unique_ptr<FileHandle> PythonFilesystem::OpenFile(const string &path, FileOpenFlags flags,
                                                  optional_ptr<FileOpener> opener) {
	PythonGILWrapper gil;

	if (flags.Compression() != FileCompressionType::UNCOMPRESSED) {
		throw IOException("Compression not supported");
	}
	// maybe this can be implemented in a better way?
	if (flags.ReturnNullIfNotExists()) {
		if (!FileExists(path)) {
			return nullptr;
		}
	}

	// TODO: lock support?

	string flags_s = DecodeFlags(flags);

	const auto &handle = filesystem.attr("open")(path, nb::str(flags_s));
	return make_uniq<PythonFileHandle>(*this, path, handle);
}

int64_t PythonFilesystem::Write(FileHandle &handle, void *buffer, int64_t nr_bytes) {
	PythonGILWrapper gil;

	const auto &write = PythonFileHandle::GetHandle(handle).attr("write");

	auto data = nb::bytes(std::string(const_char_ptr_cast(buffer), nr_bytes));

	return nb::int_(write(data));
}
void PythonFilesystem::Write(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
	Seek(handle, location);

	Write(handle, buffer, nr_bytes);
}

int64_t PythonFilesystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes) {
	PythonGILWrapper gil;

	const auto &read = PythonFileHandle::GetHandle(handle).attr("read");

	string data = nb::bytes(read(nr_bytes));

	memcpy(buffer, data.c_str(), data.size());

	return data.size();
}

void PythonFilesystem::Read(duckdb::FileHandle &handle, void *buffer, int64_t nr_bytes, uint64_t location) {
	Seek(handle, location);

	Read(handle, buffer, nr_bytes);
}
bool PythonFilesystem::FileExists(const string &filename, optional_ptr<FileOpener> opener) {
	return Exists(filename, "isfile");
}
bool PythonFilesystem::Exists(const string &filename, const char *func_name) const {
	PythonGILWrapper gil;

	return nb::bool_(filesystem.attr(func_name)(filename));
}
vector<string> PythonFilesystem::Glob(const string &path, FileOpener *opener) {
	PythonGILWrapper gil;

	if (path.empty()) {
		return {path};
	}
	auto returner = nb::list(filesystem.attr("glob")(path));

	vector<string> results;
	auto unstrip_protocol = filesystem.attr("unstrip_protocol");
	for (auto item : returner) {
		results.push_back(nb::str(unstrip_protocol(nb::str(item))));
	}
	return results;
}
string PythonFilesystem::PathSeparator(const string &path) {
	return "/";
}
int64_t PythonFilesystem::GetFileSize(FileHandle &handle) {
	// TODO: this value should be cached on the PythonFileHandle
	PythonGILWrapper gil;

	return nb::int_(filesystem.attr("size")(handle.path));
}
void PythonFilesystem::Seek(duckdb::FileHandle &handle, uint64_t location) {
	PythonGILWrapper gil;

	auto seek = PythonFileHandle::GetHandle(handle).attr("seek");
	seek(location);
	if (PyErr_Occurred()) {
		PyErr_PrintEx(1);
		throw InvalidInputException("Python exception occurred!");
	}
}
bool PythonFilesystem::CanHandleFile(const string &fpath) {
	for (const auto &protocol : protocols) {
		if (StringUtil::StartsWith(fpath, protocol + "://")) {
			return true;
		}
	}
	return false;
}
void PythonFilesystem::MoveFile(const string &source, const string &dest, optional_ptr<FileOpener> opener) {
	PythonGILWrapper gil;

	auto move = filesystem.attr("mv");
	move(nb::str(source), nb::str(dest));
}
void PythonFilesystem::RemoveFile(const string &filename, optional_ptr<FileOpener> opener) {
	PythonGILWrapper gil;

	auto remove = filesystem.attr("rm");
	remove(nb::str(filename));
}
time_t PythonFilesystem::GetLastModifiedTime(FileHandle &handle) {
	// TODO: this value should be cached on the PythonFileHandle
	PythonGILWrapper gil;

	auto last_mod = filesystem.attr("modified")(handle.path);

	return nb::int_(last_mod.attr("timestamp")());
}
void PythonFilesystem::FileSync(FileHandle &handle) {
	PythonGILWrapper gil;

	PythonFileHandle::GetHandle(handle).attr("flush")();
}
bool PythonFilesystem::DirectoryExists(const string &directory, optional_ptr<FileOpener> opener) {
	return Exists(directory, "isdir");
}
void PythonFilesystem::RemoveDirectory(const string &directory, optional_ptr<FileOpener> opener) {
	PythonGILWrapper gil;

	filesystem.attr("rm")(directory, nb::arg("recursive") = true);
}
void PythonFilesystem::CreateDirectory(const string &directory, optional_ptr<FileOpener> opener) {
	PythonGILWrapper gil;

	filesystem.attr("mkdir")(nb::str(directory));
}
bool PythonFilesystem::ListFiles(const string &directory, const std::function<void(const string &, bool)> &callback,
                                 FileOpener *opener) {
	static nb::str DIRECTORY("directory");

	PythonGILWrapper gil;
	bool nonempty = false;

	for (auto item : filesystem.attr("ls")(nb::str(directory))) {
		bool is_dir = DIRECTORY.equal(item["type"]);
		callback(nb::str(item["name"]), is_dir);
		nonempty = true;
	}

	return nonempty;
}
void PythonFilesystem::Truncate(FileHandle &handle, int64_t new_size) {
	PythonGILWrapper gil;

	filesystem.attr("touch")(handle.path, nb::arg("truncate") = true);
}
bool PythonFilesystem::IsPipe(const string &filename, optional_ptr<FileOpener> opener) {
	return false;
}
idx_t PythonFilesystem::SeekPosition(FileHandle &handle) {
	PythonGILWrapper gil;

	return nb::int_(PythonFileHandle::GetHandle(handle).attr("tell")());
}
} // namespace duckdb
