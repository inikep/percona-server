/* Copyright (c) 2023 Percona LLC and/or its affiliates. All rights reserved.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA */

#include "components/audit_log_filter/json_reader/file_reader_decompressing.h"

#include "components/audit_log_filter/audit_error_log.h"

#include <string>

namespace audit_log_filter::json_reader {

FileReaderDecompressing::FileReaderDecompressing(
    std::unique_ptr<FileReaderBase> file_reader)
    : FileReaderDecoratorBase(std::move(file_reader)) {}

FileReaderDecompressing::~FileReaderDecompressing() { close(); }

bool FileReaderDecompressing::init() noexcept {
  return FileReaderDecoratorBase::init();
}

bool FileReaderDecompressing::open(FileInfo *file_info) noexcept {
  if (!FileReaderDecoratorBase::open(file_info)) {
    return false;
  }

  m_strm.zalloc = Z_NULL;
  m_strm.zfree = Z_NULL;
  m_strm.opaque = Z_NULL;
  m_strm.next_in = m_in_buff;
  m_strm.avail_in = 0;
  m_in_eof = false;
  m_stream_end = false;

  auto ret = inflateInit2(&m_strm, MAX_WBITS + 16);

  if (ret != Z_OK) {
    LogComponentErr(ERROR_LEVEL, ER_AUDIT_DECOMPRESSION_INIT_FAILURE, ret);
    FileReaderDecoratorBase::close();
    return false;
  }

  is_opened = true;

  return true;
}

void FileReaderDecompressing::close() noexcept {
  if (is_opened) {
    is_opened = false;
    inflateEnd(&m_strm);
    FileReaderDecoratorBase::close();
  }
}

bool FileReaderDecompressing::read_input() noexcept {
  size_t in_size = 0;
  const auto status =
      FileReaderDecoratorBase::read(m_in_buff, kInBufferSize, &in_size);

  if (status == ReadStatus::Error) {
    return false;
  }

  m_in_eof = status == ReadStatus::Eof;
  m_strm.next_in = m_in_buff;
  m_strm.avail_in = static_cast<uInt>(in_size);

  return true;
}

ReadStatus FileReaderDecompressing::read(unsigned char *out_buffer,
                                         const size_t out_buffer_size,
                                         size_t *read_size) noexcept {
  *read_size = 0;

  if (m_stream_end) {
    return ReadStatus::Eof;
  }

  m_strm.next_out = out_buffer;
  m_strm.avail_out = static_cast<uInt>(out_buffer_size);

  // The last input chunk may inflate to much more than one output buffer, so
  // keep inflating until the output buffer is full or the stream ends
  while (m_strm.avail_out > 0) {
    if (m_strm.avail_in == 0 && !m_in_eof && !read_input()) {
      return ReadStatus::Error;
    }

    const int ret = inflate(&m_strm, Z_NO_FLUSH);

    if (ret == Z_STREAM_END) {
      m_stream_end = true;
      break;
    }

    if (ret == Z_BUF_ERROR && m_strm.avail_in == 0) {
      if (m_in_eof) {
        // No gzip trailer, as in the current log or a log left by a crash,
        // return what could be decompressed
        m_stream_end = true;
        break;
      }
      continue;
    }

    if (ret != Z_OK) {
      return ReadStatus::Error;
    }
  }

  *read_size = out_buffer_size - m_strm.avail_out;

  return m_stream_end ? ReadStatus::Eof : ReadStatus::Ok;
}

}  // namespace audit_log_filter::json_reader
