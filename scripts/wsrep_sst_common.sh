# Copyright (C) 2012-2015 Codership Oy
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; version 2 of the License.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; see the file COPYING. If not, write to the
# Free Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston
# MA  02110-1301  USA.

# This is a common command line parser to be sourced by other SST scripts

wsrep_log()
{
    # echo everything to stderr so that it gets into common error log
    # deliberately made to look different from the rest of the log
    local readonly tst="$(date +%Y%m%d\ %H:%M:%S.%N | cut -b -21)"
    echo "WSREP_SST: $* ($tst)" >&2
}

wsrep_log_error()
{
    wsrep_log "[ERROR] $*"
}

wsrep_log_warning()
{
    wsrep_log "[WARNING] $*"
}

wsrep_log_info()
{
    wsrep_log "[INFO] $*"
}

wsrep_log_debug()
{
    if [[ -n "$WSREP_LOG_DEBUG" ]]; then
        wsrep_log "DBG:(debug) $*"
    fi
}

set -u

WSREP_SST_OPT_BYPASS=0
WSREP_SST_OPT_BINLOG=""
WSREP_SST_OPT_BINLOG_INDEX=""
WSREP_SST_OPT_LOCAL_GTID=""
WSREP_SST_OPT_SERVER_ID=0
WSREP_SST_OPT_CONF=""
WSREP_SST_OPT_CONF_SUFFIX=""
WSREP_SST_OPT_DATA=""
WSREP_SST_OPT_AUTH=${WSREP_SST_OPT_AUTH:-}
WSREP_SST_OPT_USER=${WSREP_SST_OPT_USER:-}
WSREP_SST_OPT_PSWD=${WSREP_SST_OPT_PSWD:-}
WSREP_SST_OPT_REMOTE_AUTH=${WSREP_SST_OPT_REMOTE_AUTH:-}
WSREP_SST_OPT_VERSION=""
WSREP_SST_OPT_DEFAULT=""
WSREP_SST_OPT_EXTRA_DEFAULT=""
WSREP_SST_OPT_SUFFIX_DEFAULT=""
WSREP_SST_OPT_PLUGIN_DIR=""
INNODB_DATA_HOME_DIR_ARG=""

readonly WSREP_SST_OPT_REMOTE_AUTH

while [ $# -gt 0 ]; do
case "$1" in
    '--address')
        readonly WSREP_SST_OPT_ADDR="$2"
        #
        # Break address string into host:port/path parts
        #
        case "${WSREP_SST_OPT_ADDR}" in
        \[*)
            # IPv6
            addr_no_bracket=${WSREP_SST_OPT_ADDR#\[}
            readonly WSREP_SST_OPT_HOST_UNESCAPED=${addr_no_bracket%%\]*}
            readonly WSREP_SST_OPT_HOST="[${WSREP_SST_OPT_HOST_UNESCAPED}]"
            readonly WSREP_SST_OPT_HOST_ESCAPED="\\[${WSREP_SST_OPT_HOST_UNESCAPED}\\]"
            ;;
        *)
            readonly WSREP_SST_OPT_HOST=${WSREP_SST_OPT_ADDR%%[:/]*}
            readonly WSREP_SST_OPT_HOST_UNESCAPED=$WSREP_SST_OPT_HOST
            readonly WSREP_SST_OPT_HOST_ESCAPED=$WSREP_SST_OPT_HOST
            ;;
        esac
        remain=${WSREP_SST_OPT_ADDR#${WSREP_SST_OPT_HOST_ESCAPED}}
        remain=${remain#:}
        readonly WSREP_SST_OPT_ADDR_PORT=${remain%%/*}
        remain=${remain#*/}
        readonly WSREP_SST_OPT_MODULE=${remain%%/*}
        readonly WSREP_SST_OPT_PATH=${WSREP_SST_OPT_ADDR#*/}
        remain=${WSREP_SST_OPT_PATH#*/}
        if [ "$remain" != "${WSREP_SST_OPT_PATH}" ]; then
            readonly WSREP_SST_OPT_LSN=${remain%%/*}
            remain=${remain#*/}
            if [ "$remain" != "${WSREP_SST_OPT_LSN}" ]; then
                readonly WSREP_SST_OPT_SST_VER=${remain%%/*}
            else
                readonly WSREP_SST_OPT_SST_VER=""
            fi
        else
            readonly WSREP_SST_OPT_LSN=""
            readonly WSREP_SST_OPT_SST_VER=""
        fi
        shift
        ;;
    '--bypass')
        WSREP_SST_OPT_BYPASS=1
        ;;
    '--datadir')
        # strip trailing '/'
        readonly WSREP_SST_OPT_DATA="${2%/}"
        shift
        ;;
    '--innodb-data-home-dir')
        readonly INNODB_DATA_HOME_DIR_ARG="$2"
        shift
        ;;
    '--defaults-file')
        readonly WSREP_SST_OPT_CONF="$2"
        shift
        ;;
    '--defaults-group-suffix')
        WSREP_SST_OPT_CONF_SUFFIX="$2"
        shift
        ;;
    '--defaults-extra-file')
        readonly WSREP_SST_OPT_EXTRA_DEFAULT="$1=$2"
        shift
        ;;
    '--defaults-group-suffix')
        readonly WSREP_SST_OPT_SUFFIX_DEFAULT="$1=$2"
        shift
        ;;
    '--host')
        readonly WSREP_SST_OPT_HOST="$2"
        shift
        ;;
    '--local-port')
        readonly WSREP_SST_OPT_LPORT="$2"
        shift
        ;;
    '--parent')
        readonly WSREP_SST_OPT_PARENT="$2"
        shift
        ;;
    '--password')
        WSREP_SST_OPT_PSWD="$2"
        shift
        ;;
    '--port')
        readonly WSREP_SST_OPT_PORT="$2"
        shift
        ;;
    '--role')
        readonly WSREP_SST_OPT_ROLE="$2"
        shift
        ;;
    '--socket')
        readonly WSREP_SST_OPT_SOCKET="$2"
        shift
        ;;
    '--user')
        WSREP_SST_OPT_USER="$2"
        shift
        ;;
    '--mysqld-version')
        readonly WSREP_SST_OPT_VERSION="$2"
        shift
        ;;
    '--gtid')
        readonly WSREP_SST_OPT_GTID="$2"
        shift
        ;;
    '--binlog')
        WSREP_SST_OPT_BINLOG="$2"
        shift
        ;;
    '--binlog-index')
        WSREP_SST_OPT_BINLOG_INDEX="$2"
        shift
        ;;
    '--local-gtid')
        WSREP_SST_OPT_LOCAL_GTID="$2"
        shift
        ;;
    '--server-id')
        WSREP_SST_OPT_SERVER_ID="$2"
        shift
        ;;
    '--plugin-dir')
        readonly WSREP_SST_OPT_PLUGIN_DIR="$2"
        shift
        ;;
    *) # must be command
       # usage
       # exit 1
       ;;
esac
shift
done

readonly WSREP_SST_OPT_BYPASS
readonly WSREP_SST_OPT_BINLOG
readonly WSREP_SST_OPT_BINLOG_INDEX
readonly WSREP_SST_OPT_LOCAL_GTID
readonly WSREP_SST_OPT_SERVER_ID

if [ -n "${WSREP_SST_OPT_ADDR_PORT:-}" ]; then
  if [ -n "${WSREP_SST_OPT_PORT:-}" ]; then
    if [ "$WSREP_SST_OPT_PORT" != "$WSREP_SST_OPT_ADDR_PORT" ]; then
      echo "WSREP_SST: [ERROR] port in --port=$WSREP_SST_OPT_PORT differs from port in --address=$WSREP_SST_OPT_ADDR" >&2
      exit 2
    fi
  else
    readonly WSREP_SST_OPT_PORT="$WSREP_SST_OPT_ADDR_PORT"
  fi
fi

# try to use my_print_defaults, mysql and mysqldump that come with the sources
# (for MTR suite)
SCRIPTS_DIR="$(cd $(dirname "$0"); pwd -P)"
EXTRA_DIR="$SCRIPTS_DIR/../extra"
BIN_DIR="$SCRIPTS_DIR/../bin"

if [ -x "$BIN_DIR/mysql" ]; then
    MYSQL_CLIENT="$BIN_DIR/mysql"
else
    MYSQL_CLIENT=mysql
fi

if [ -x "$BIN_DIR/mysqldump" ]; then
    MYSQLDUMP="$BIN_DIR/mysqldump"
else
    MYSQLDUMP=mysqldump
fi

if [ -x "$SCRIPTS_DIR/my_print_defaults" ]; then
    MY_PRINT_DEFAULTS="$SCRIPTS_DIR/my_print_defaults"
elif [ -x "$EXTRA_DIR/my_print_defaults" ]; then
    MY_PRINT_DEFAULTS="$EXTRA_DIR/my_print_defaults"
else
    MY_PRINT_DEFAULTS=my_print_defaults
fi

if [ -n "$WSREP_SST_OPT_CONF" ]
then
    readonly MY_PRINT_DEFAULTS="$MY_PRINT_DEFAULTS --defaults-file=$WSREP_SST_OPT_CONF"
else
    readonly MY_PRINT_DEFAULTS
fi

wsrep_auth_not_set()
{
    [ -z "$WSREP_SST_OPT_AUTH" -o "$WSREP_SST_OPT_AUTH" = "(null)" ]
}

# State Snapshot Transfer authentication password was displayed in the ps output. Bug fixed #1200727.
if $MY_PRINT_DEFAULTS sst | grep -q "wsrep_sst_auth"; then
    if wsrep_auth_not_set; then
        WSREP_SST_OPT_AUTH=$($MY_PRINT_DEFAULTS sst | grep -- "--wsrep_sst_auth" | cut -d= -f2)
    fi
fi
readonly WSREP_SST_OPT_AUTH

# Splitting AUTH into potential user:password pair
if ! wsrep_auth_not_set
then
    # Split auth string at the last ':'
    WSREP_SST_OPT_USER="${WSREP_SST_OPT_AUTH%:*}"
    if [ "$WSREP_SST_OPT_USER" != "$WSREP_SST_OPT_AUTH" ]
    then
        WSREP_SST_OPT_PSWD="${WSREP_SST_OPT_AUTH##*:}"
    else
        WSREP_SST_OPT_PSWD=""
    fi
fi
readonly WSREP_SST_OPT_USER
readonly WSREP_SST_OPT_PSWD

if [ -n "$WSREP_SST_OPT_REMOTE_AUTH" ]
then
    # Split auth string at the last ':'
    readonly WSREP_SST_OPT_REMOTE_USER="${WSREP_SST_OPT_REMOTE_AUTH%:*}"
    readonly WSREP_SST_OPT_REMOTE_PSWD="${WSREP_SST_OPT_REMOTE_AUTH##*:}"
else
    readonly WSREP_SST_OPT_REMOTE_USER=
    readonly WSREP_SST_OPT_REMOTE_PSWD=
fi

if [ -n "$WSREP_SST_OPT_DATA" ]
then
    SST_PROGRESS_FILE="$WSREP_SST_OPT_DATA/sst_in_progress"
else
    SST_PROGRESS_FILE=""
fi

# Returns the version string in a standardized format
# Input "1.2.3" => echoes "010203"
# Wrongly formatted values => echoes "000000"
#
# Globals:
#   None
#
# Arguments:
#   Parameter 1: a version string
#                like "5.1.12"
#                anything after the major.minor.revision is ignored
# Outputs:
#   A string that can be used directly with string comparisons.
#   So, the string "5.1.12" is transformed into "050112"
#   Note that individual version numbers can only go up to 99.
#
function normalize_version()
{
    local major=0
    local minor=0
    local patch=0

    # Only parses purely numeric version numbers, 1.2.3
    # Everything after the first three values are ignored
    if [[ $1 =~ ^([0-9]+)\.([0-9]+)\.?([0-9]*)([^ ])* ]]; then
        major=${BASH_REMATCH[1]}
        minor=${BASH_REMATCH[2]}
        patch=${BASH_REMATCH[3]}
    fi

    printf %02d%02d%02d $major $minor $patch
}

# Compares two version strings
#   The version strings passed in will be normalized to a
#   string-comparable version.
#
# Globals:
#   None
#
# Arguments:
#   Parameter 1: The left-side of the comparison (for example: "5.7.25")
#   Parameter 2: the comparison operation
#                   '>', '>=', '=', '==', '<', '<=', "!="
#   Parameter 3: The right-side of the comparison (for example: "5.7.24")
#
# Returns:
#   Returns 0 (success) if param1 op param2
#   Returns 1 (failure) otherwise
#
function compare_versions()
{
    local version_1="$( normalize_version $1 )"
    local op=$2
    local version_2="$( normalize_version $3 )"

    if [[ ! " = == > >= < <= != " =~ " $op " ]]; then
        wsrep_log_error "******************* ERROR ********************** "
        wsrep_log_error "Unknown operation : $op"
        wsrep_log_error "Must be one of : = == > >= < <="
        wsrep_log_error "******************* ERROR ********************** "
        return 1
    fi

    [[ $op == "<"  &&   $version_1 <  $version_2 ]] && return 0
    [[ $op == "<=" && ! $version_1 >  $version_2 ]] && return 0
    [[ $op == "="  &&   $version_1 == $version_2 ]] && return 0
    [[ $op == "==" &&   $version_1 == $version_2 ]] && return 0
    [[ $op == ">"  &&   $version_1 >  $version_2 ]] && return 0
    [[ $op == ">=" && ! $version_1 <  $version_2 ]] && return 0
    [[ $op == "!=" &&   $version_1 != $version_2 ]] && return 0

    return 1
}
# Reads incoming data from STDIN and sets the variables
#
# Globals:
#   WSREP_SST_OPT_USER (sets this variable)
#   WSREP_SST_OPT_PSWD (sets this variable)
#   WSREP_SYS_SCHEMA_VERSION (sets this variable)
#   WSREP_WSREP_SCHEMA_VERSION (sets this variable)
#
# Parameters:
#   None
#
function read_variables_from_stdin()
{
    while read line; do
        name=$(echo "$line" | cut -d"=" -f1)
        value=$(echo "$line" | cut -d"=" -f2)

        case "$name" in
            'sst_user')
                WSREP_SST_OPT_USER="$value"
                ;;
            'sst_password')
                WSREP_SST_OPT_PSWD="$value"
                ;;
            'wsrep_schema_version')
                WSREP_WSREP_SCHEMA_VERSION=$(expr match "$value" '\([0-9]\+\.[0-9]\+\.[0-9]\+\)')
                #'
                ;;
            *)
                wsrep_log_warning "Unrecognized input: $line"
        esac
    done
    return 0
}


parse_cnf_group()
{
    local group=$1
    local var=$2

    # first normalize output variable names specified in cnf file:
    # user can use _ or - (for example log-bin or log_bin) and/or prefix
    # variable with --loose-
    # then search for needed variable
    # finally get the variable value (if variables has been specified multiple
    # time use the last value only)

    $MY_PRINT_DEFAULTS "${group}" | \
        awk -F= '{ sub(/^--loose/,"-",$0); \
                   if ($1 ~ /_/) \
                       { gsub(/_/,"-",$1); print $1"="$2 } \
                   else \
                      { print $0 } \
                 }' | grep -- "--$var=" | cut -d= -f2- | tail -1
}

#
# user can specify mariabackup specific settings that will be used during sst
# process like encryption, etc.....
# parse such configuration option. (group for xb settings is [sst] in my.cnf
#
# 1st param: group (config file section like sst) or my_print_defaults argument (like --mysqld)
# 2nd param: var : name of the variable in the section, e.g. server-id
# 3rd param: - : default value for the param
parse_cnf()
{
    local group=$1
    local var=${2//_/-} # normalize variable name by replacing all '_' with '-'
    local reval=""

    # look in group+suffix
    if [[ -n $WSREP_SST_OPT_CONF_SUFFIX ]]; then
        reval=$(parse_cnf_group "${group}${WSREP_SST_OPT_CONF_SUFFIX}" $var)
    fi

    # look in group
    if [[ -z $reval ]]; then
        reval=$(parse_cnf_group "${group}" $var)
    fi

    # use default if we haven't found a value
    if [[ -z $reval ]]; then
        [[ -n $3 ]] && reval=$3
    fi
    echo $reval
}

#
# Converts an input string into a boolean "on", "off"
# Converts:
#   "on", "true", "1" -> "on" (case insensitive)
#   "off", "false", "0" -> "off" (case insensitive)
# All other values : -> default_value
#
# 1st param: input value
# 2nd param: default value
normalize_boolean()
{
    local input_value=$1
    local default_value=$2

    [[ "$input_value" == "1" ]]                 && echo "on"    && return 0
    [[ "$input_value" =~ ^[Oo][Nn]$ ]]          && echo "on"    && return 0
    [[ "$input_value" =~ ^[Tt][Rr][Uu][Ee]$ ]]  && echo "on"    && return 0
    [[ "$input_value" == "0" ]]                 && echo "off"   && return 0
    [[ "$input_value" =~ ^[Oo][Ff][Ff]$ ]]      && echo "off"   && return 0
    [[ "$input_value" =~ ^[Ff][Aa][Ll][Ss][Ee]$ ]] && echo "off" && return 0

    echo $default_value
    return 0
}

WSREP_LOG_DEBUG=$(parse_cnf sst wsrep-debug "")
if [ -z "$WSREP_LOG_DEBUG" ]; then
    WSREP_LOG_DEBUG=$(parse_cnf mysqld wsrep-debug "")
fi
WSREP_LOG_DEBUG=$(normalize_boolean "$WSREP_LOG_DEBUG" "off")
if [[ "$WSREP_LOG_DEBUG" == "off" ]]; then
    WSREP_LOG_DEBUG=""
fi

wsrep_cleanup_progress_file()
{
    [ -n "${SST_PROGRESS_FILE:-}" ] && rm -f "$SST_PROGRESS_FILE" 2>/dev/null || true
}

wsrep_check_program()
{
    local prog=$1

    if ! command -v $prog >/dev/null
    then
        echo "'$prog' not found in PATH"
        return 2 # ENOENT no such file or directory
    fi
}

wsrep_check_programs()
{
    local ret=0

    while [ $# -gt 0 ]
    do
        wsrep_check_program $1 || ret=$?
        shift
    done

    return $ret
}

# Generate a string equivalent to 16 random bytes
wsrep_gen_secret()
{
    if [ -x /usr/bin/openssl ]
    then
        echo `/usr/bin/openssl rand -hex 16`
    else
        printf "%04x%04x%04x%04x%04x%04x%04x%04x" \
                $RANDOM $RANDOM $RANDOM $RANDOM   \
                $RANDOM $RANDOM $RANDOM $RANDOM
    fi
}

# Takes a file and writes it to stderr in a way that allows
# for better formatting by the server
#
# Globals:
#   None
#
# Parameters:
#   Argument 1: Path to the file
#   Argument 2: Priority level (DBG, INF, WRN, ERR)
#   Argument 3: File description
#
function cat_file_to_stderr()
{
    local file_path=$1
    local level=$2
    local description=$3

    echo "FIL:${level}:${description}" >&2
    cat $file_path >&2
    echo "EOF:" >&2
}

# timeout for donor to connect to joiner (seconds)
readonly WSREP_SST_DONOR_TIMEOUT=$(parse_cnf sst donor-timeout 10)
# For backward compatiblitiy: joiner timeout waiting for donor connection
readonly WSREP_SST_JOINER_TIMEOUT=$(parse_cnf sst joiner-timeout $(parse_cnf sst sst-initial-timeout 60) )
