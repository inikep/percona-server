/* Copyright 2021 Codership Oy <info@codership.com>

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

#include "wsrep_status.h"

mysql_mutex_t LOCK_wsrep_status;

#ifdef HAVE_PSI_INTERFACE
PSI_mutex_key key_LOCK_wsrep_status;
#endif

Wsrep_mutex*     Wsrep_status::m_mutex    = 0;
wsrep::reporter* Wsrep_status::m_instance = 0;

void Wsrep_status::init_once(const std::string& file_name)
{
  if (file_name.length() > 0 && m_instance == 0)
  {
    mysql_mutex_init(key_LOCK_wsrep_status, &LOCK_wsrep_status,
                     MY_MUTEX_INIT_FAST);
    m_mutex    = new Wsrep_mutex(LOCK_wsrep_status);
    // 4 is an ad-hoc value that seemed reasonable to hardcode.
    // Ideally an additinal configuration variable should be introduced.
    m_instance = new wsrep::reporter(*m_mutex, file_name, 4);
  }
}

void Wsrep_status::destroy()
{
  if (m_instance)
  {
    delete m_instance;
    m_instance= 0;
    delete m_mutex;
    m_mutex= 0;
    mysql_mutex_destroy(&LOCK_wsrep_status);
  }
}
