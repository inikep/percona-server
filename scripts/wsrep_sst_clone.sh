#!/usr/bin/env bash

# Copyright (C) 2020 Codership Oy
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

# This is a reference script for clone-based state snapshot tansfer

CMDLINE="$@"

set -o nounset -o errexit

readonly EBUSY=16
readonly EINVAL=22
readonly ETIMEDOUT=110

# wsrep_gen_secret() generates enough randomness, yet some MySQL password
# policy may insist on having upper and lower case letters, numbers and
# special symbols. Make sure that whatever we generate, it has it.
readonly PSWD_POLICY="Aa+1"

OS=$(uname)
[ "$OS" = "Darwin" ] && export -n LD_LIBRARY_PATH

. $(dirname $0)/wsrep_sst_common

wsrep_log_info "Running: $CMDLINE"

cleanup_donor()
{
    export MYSQL_PWD=$ADMIN_PSWD
    if [ "$CLEANUP_CLONE_PLUGIN" ]
    then
        CLEANUP_CLONE_PLUGIN="UNINSTALL PLUGIN CLONE;"
    else
        if [ "$CLEANUP_CLONE_SSL" ]
        then
            $MYSQL_ACLIENT -e "SET GLOBAL clone_ssl_cert='';
                               SET GLOBAL clone_ssl_key='';
                               SET GLOBAL clone_ssl_ca='';"
        fi
    fi

    $MYSQL_ACLIENT -e "DROP USER IF EXISTS '$CLONE_USER'; \
                       SET wsrep_on = OFF; $CLEANUP_CLONE_PLUGIN"

    [ "$CLONE_PREPARE_SQL" ] && rm -rf "$CLONE_PREPARE_SQL" ||:
    [ "$CLONE_EXECUTE_SQL" ] && rm -rf "$CLONE_EXECUTE_SQL" ||:
}

cleanup_joiner()
{
    wsrep_log_info "Joiner cleanup. SST daemon PID: $CLONE_REAL_PID"
    [ "0" != "$CLONE_REAL_PID" ]            && \
    kill $CLONE_REAL_PID                    && \
    sleep 5                                 && \
    kill -9 $CLONE_REAL_PID >/dev/null 2>&1 || :

    rm -rf $CLONE_SOCK_DIR
    rm -rf $CLONE_PID_FILE
    if [ $CLEANUP_FILES ]
    then
        rm -rf $CLONE_ERR
        rm -rf $CLONE_SQL
    else
        # for easier troubleshooting merge in receiver error log
        wsrep_log_info "################# Clone recipient error log ##################"
        cat $CLONE_ERR >> /dev/stderr
        wsrep_log_info "################# /Clone recipient error log ##################"
    fi
    wsrep_log_info "Joiner cleanup done."
}

# Check whether process in PID file is still running.
check_pid_file()
{
    local pid_file=$1
    [ -r "$pid_file" ] && ps -p $(cat $pid_file) >/dev/null 2>&1
}

check_parent()
{
    local parent_pid=$1
    if ! ps -p $parent_pid >/dev/null
    then
        wsrep_log_error \
        "Parent mysqld process (PID:$parent_pid) terminated unexpectedly."
#        kill -- -"${parent_pid}"
#        sleep 1
        exit 32
    fi
}

# Check client version
check_client_version()
{
    local readonly min_version="8.0.19"
    IFS="." read -ra min_vers <<< $min_version # split into major minor and patch

    local readonly client_version=${1%%[-/]*} # take only a.b.c from a.b.c-d.e
    IFS="." read -ra client_vers <<< $client_version

    for i in ${!min_vers[@]}
    do
        if [ "${client_vers[$i]}" -lt "${min_vers[$i]}" ]
        then
            wsrep_log_error "this operation requires MySQL client version $min_version," \
                            " this client is '$client_version'"
            return $EINVAL
        fi
    done
}

trim_to_mysql_user_length()
{
    local readonly max_length=32
    local input=$1
    local readonly input_length=${#input}

    if [ $input_length -gt $max_length ]
    then
        local readonly tail_length=6 # to fit port completely
        local readonly head_length=$(($max_length - $tail_length))
        local readonly tail_offset=$(($input_length - $tail_length + 1))
        local readonly head_str=$(echo $input | cut -c -$head_length)
        local readonly tail_str=$(echo $input | cut -c $tail_offset-)
        input=$head_str$tail_str
    fi

    # here we potentially may want to filter out "bad" characters
    echo $input
}

# The following function:
# 1. checks if CLONE plugin is loaded. If not loads it.
# 2. checks if SSL cert and key are configured for any of and in that order:
#    a. CLONE plugin
#    b. MySQL in general
#    c. in [sst] section of my.cnf
# 3. If SSL is configured, but not for the CLONE plugin explicitly, sets up
#    corresponging CLONE plugin variables
#
# Requires environment variables: MYSQL_ACLIENT, MYSQL_PWD
# Sets the following environment variables:
# CLENUP_PLUGIN, WITH_OPTION, REQUIRE_SSL, CLONE_SSL_CERT,
# CLONE_SSL_KEY, CLONE_SSL_CA, CLIENT_SSL_OPTIONS, CLEANUP_SSL
#
setup_clone_plugin()
{
    # Either donor or recipient
    local -r ROLE=$1

    local CLONE_PLUGIN_LOADED=`$MYSQL_ACLIENT -e "SELECT COUNT(*) FROM INFORMATION_SCHEMA.PLUGINS WHERE PLUGIN_TYPE = 'CLONE';"`
    if [ "$CLONE_PLUGIN_LOADED" -eq 0 ]
    then
        wsrep_log_info "Installing CLONE plugin"

        # INSTALL PLUGIN is replicated by default, so we need to switch off
        # session replication on donor
        if [ "$ROLE" = "donor" ]
        then
            WSREP_OFF="SET SESSION wsrep_on=OFF; "
        else
            WSREP_OFF="" # joiner does not have replication enabled
        fi
        $MYSQL_ACLIENT -e "${WSREP_OFF}INSTALL PLUGIN CLONE SONAME 'mysql_clone.so';"
        CLEANUP_CLONE_PLUGIN="yes"
        CLONE_SSL_CERT="NULL"
        CLONE_SSL_KEY="NULL"
        CLONE_SSL_CA="NULL"
    else
        CLEANUP_CLONE_PLUGIN=
        CLONE_SSL_CERT=`$MYSQL_ACLIENT -e "SELECT @@clone_ssl_cert"`
        CLONE_SSL_KEY=`$MYSQL_ACLIENT -e "SELECT @@clone_ssl_key"`
        CLONE_SSL_CA=`$MYSQL_ACLIENT -e "SELECT @@clone_ssl_ca"`
    fi

    local CLIENT_SSL_CERT=$(parse_cnf sst ssl_cert "")
    local CLIENT_SSL_KEY=$(parse_cnf sst ssl_key "")
    local CLIENT_SSL_CA=$(parse_cnf sst ssl_ca "")
    local CLIENT_SSL_MODE=$(parse_cnf sst ssl_mode "")

    if [ -z "$CLIENT_SSL_CERT" -o -z "$CLIENT_SSL_KEY" ]
    then
        CLIENT_SSL_CERT=$(parse_cnf client ssl_cert "")
        CLIENT_SSL_KEY=$(parse_cnf client ssl_key "")
        CLIENT_SSL_CA=$(parse_cnf client ssl_ca "")
        CLIENT_SSL_MODE=$(parse_cnf client ssl_mode "")
    fi

    local SERVER_SSL_CERT=`$MYSQL_ACLIENT -e "SELECT @@ssl_cert"`
    local SERVER_SSL_KEY=`$MYSQL_ACLIENT -e "SELECT @@ssl_key"`
    local SERVER_SSL_CA=`$MYSQL_ACLIENT -e "SELECT @@ssl_ca"`

    [ "$SERVER_SSL_CERT" = "NULL" ] && SERVER_SSL_CERT=
    [ "$SERVER_SSL_KEY" = "NULL" ] && SERVER_SSL_KEY=
    [ "$SERVER_SSL_CA" = "NULL" ] && SERVER_SSL_CA=

    if [ "$CLONE_SSL_CERT" = "NULL" -o "$CLONE_SSL_KEY" = "NULL" ]
    then
        wsrep_log_info "CLONE SSL not configured. Checking general MySQL SSL settings."

        if [ "$ROLE" = "donor" ]
        then
            CLONE_SSL_CERT=$SERVER_SSL_CERT
            CLONE_SSL_KEY=$SERVER_SSL_KEY
            CLONE_SSL_CA=$SERVER_SSL_CA
        else
            CLONE_SSL_CERT=$CLIENT_SSL_CERT
            CLONE_SSL_KEY=$CLIENT_SSL_KEY
            CLONE_SSL_CA=$CLIENT_SSL_CA
        fi

        if [ -n "$CLONE_SSL_CERT" -a -n "$CLONE_SSL_KEY" ]
        then
            wsrep_log_info "Using SSL configuration from MySQL Server."
            $MYSQL_ACLIENT -e "SET GLOBAL clone_ssl_cert='$CLONE_SSL_CERT'"
            $MYSQL_ACLIENT -e "SET GLOBAL clone_ssl_key='$CLONE_SSL_KEY'"
            $MYSQL_ACLIENT -e "SET GLOBAL clone_ssl_ca='$CLONE_SSL_CA'"
            CLEANUP_CLONE_SSL="yes"
        fi
    else
        wsrep_log_info "CLONE SSL already configured. Using it."
        CLEANUP_CLONE_SSL=
    fi

    CLIENT_SSL_OPTIONS=
    if [ -n "$CLONE_SSL_CERT" -a -n "$CLONE_SSL_KEY" ]
    then
        wsrep_log_info "Server SSL settings on $ROLE: CERT=$SERVER_SSL_CERT, KEY=$SERVER_SSL_KEY, CA=$SERVER_SSL_CA"
        wsrep_log_info "Client SSL settings on $ROLE: CERT=$CLIENT_SSL_CERT, KEY=$CLIENT_SSL_KEY, CA=$CLIENT_SSL_CA"
        wsrep_log_info "CLONE SSL settings on $ROLE: CERT=$CLONE_SSL_CERT, KEY=$CLONE_SSL_KEY, CA=$CLONE_SSL_CA"
        REQUIRE_SSL="REQUIRE SSL"

        if [ -n "$CLIENT_SSL_CERT" -a -n "$CLIENT_SSL_KEY" ]
        then
            CLIENT_SSL_OPTIONS="--ssl-cert=$CLIENT_SSL_CERT --ssl-key=$CLIENT_SSL_KEY"
            if [ -n "$CLIENT_SSL_CA"   ]
            then
                CLIENT_SSL_OPTIONS+=" --ssl-ca=$CLIENT_SSL_CA"
                [ -n "$CLIENT_SSL_MODE" ] && CLIENT_SSL_OPTIONS+=" --ssl-mode=$CLIENT_SSL_MODE"
            else
                CLIENT_SSL_OPTIONS+=" --ssl-mode=REQUIRED"
            fi
        fi
        WITH_OPTION=
    else
        wsrep_log_info "No SSL configuration found. Not using SSL for SST."
        CLEANUP_CLONE_SSL=
        REQUIRE_SSL=
        # needed to create clone user on recepient for insecure connections
        WITH_OPTION="WITH mysql_native_password"
    fi
}

progress_monitor_setup()
{
    local mysql=$1
    local user=$2
    local socket=$3

    $mysql -u"$user" -S"$socket" --silent --batch --skip-column-names -e \
        "UPDATE performance_schema.setup_instruments SET ENABLED = 'YES'
        WHERE NAME LIKE 'stage/innodb/clone%';" >> /dev/stderr

    $mysql -u"$user" -S"$socket" --silent --batch --skip-column-names -e \
        "UPDATE performance_schema.setup_consumers SET ENABLED = 'YES'
        WHERE NAME LIKE '%stages%';" >> /dev/stderr
}

# Return progress
progress_table_contents()
{
    local mysql=$1
    local user=$2
    local socket=$3
    local table=$4

    $mysql -u"$user" -S"$socket" --batch --skip-column-names -e \
        "SELECT EVENT_NAME, WORK_COMPLETED, WORK_ESTIMATED FROM
        performance_schema.events_stages_$table
        WHERE EVENT_NAME LIKE 'stage/innodb/clone%';"
}

progress_monitor_run()
{
    local mysql=$1
    local user=$2
    local socket=$3

    local stage=
    local count=1
    while [ 1 ]
    do
        out=( $(progress_table_contents "$mysql" "$user" "$socket" "current") )

        if [ ${#out[@]} -eq 5 ]
        then
            new_stage="${out[1]} ${out[2]}"
            complete=${out[3]}
            total=${out[4]}
            if [ "$new_stage" != "$stage" ]
            then
                stage="$new_stage"
                echo "total $total" >> /dev/stdout
            fi
            echo "complete $complete" >> /dev/stdout
        elif [ ${#out[@]} -gt 0 ]
        then
            wsrep_log_info "Unexpected output of ${#out[@]}: " ${out[@]}
        fi

        sleep 0.5
    done
}

if test -z "$WSREP_SST_OPT_HOST"; then wsrep_log_error "HOST cannot be nil"; exit $EINVAL; fi
# MySQL client does not seem to agree to [] around IPv6 addresses
wsrep_check_programs sed
SST_HOST_STRIPPED=$(echo $WSREP_SST_OPT_HOST | sed 's/^\[//' | sed 's/\]$//')

PROGRESS=$(parse_cnf sst progress "")
if [ -n "$PROGRESS" -a "$PROGRESS" != "none" ]
then
    wsrep_log_warning "Unrecognized SST progress parameter value: '$PROGRESS'" \
                      "Recognized values are 'none' - disable progress reporting " \
                      "and empty string - enable progress reporting (default)"
                      "SST progress reporting will be disabled"
    PROGRESS="none"
fi

if [ "$WSREP_SST_OPT_ROLE" = "donor" ]
then
    echo "continue" # donor can resume updating data

    if test -z "$WSREP_SST_OPT_USER";   then wsrep_log_error "USER cannot be nil";   exit $EINVAL; fi
    if test -z "$WSREP_SST_OPT_REMOTE_USER"; then wsrep_log_error "PSWD cannot be nil";   exit $EINVAL; fi
    if test -z "$WSREP_SST_OPT_PORT";   then wsrep_log_error "PORT cannot be nil"; exit $EINVAL; fi
    if test -z "$WSREP_SST_OPT_LPORT";  then wsrep_log_error "LPORT cannot be nil";  exit $EINVAL; fi
    if test -z "$WSREP_SST_OPT_SOCKET"; then wsrep_log_error "SOCKET cannot be nil"; exit $EINVAL; fi

    CLIENT_VERSION=$($MYSQL_CLIENT --version | grep -vi MariaDB | cut -d ' ' -f 4)
    check_client_version $CLIENT_VERSION

    readonly RECIPIENT_USER=$WSREP_SST_OPT_REMOTE_USER
    readonly RECIPIENT_PSWD=$WSREP_SST_OPT_REMOTE_PSWD
    readonly ADMIN_USER=$WSREP_SST_OPT_USER
    readonly ADMIN_PSWD=$WSREP_SST_OPT_PSWD

    MYSQL_RCLIENT="$MYSQL_CLIENT -u$RECIPIENT_USER -h$SST_HOST_STRIPPED \
                   -P$WSREP_SST_OPT_PORT --batch --skip_column_names --silent"
    MYSQL_ACLIENT="$MYSQL_CLIENT -u$ADMIN_USER -S$WSREP_SST_OPT_SOCKET \
                   --batch --skip_column_names --silent"

    if [ $WSREP_SST_OPT_BYPASS -eq 0 ]
    then
        #
        #  Prepare DONOR for cloning
        #
        CLEANUP_CLONE_PLUGIN=
        CLEANUP_CLONE_SSL=
        CLONE_PREPARE_SQL=
        CLONE_EXECUTE_SQL=
        CLONE_USER=`trim_to_mysql_user_length clone_to_${WSREP_SST_OPT_HOST}:${WSREP_SST_OPT_PORT}`
        CLONE_PSWD=`wsrep_gen_secret`$PSWD_POLICY

        cleanup_donor # Remove potentially existing clone user

        export MYSQL_PWD=$ADMIN_PSWD
        setup_clone_plugin "donor"
        wsrep_log_info "REQUIRE_SSL=$REQUIRE_SSL, CLIENT_SSL_OPTIONS=$CLIENT_SSL_OPTIONS"

        # Server configuration might have changed, make sure it is restored
        # on exit
        trap cleanup_donor EXIT

        # It would be nice to limit clone user connections to only from
        # the recipient, however, even though we know the recipient address
        # there is no guarantee that when connecting from it source address
        # will be resolved to the same value. Hence we need to allow
        # connection from anywhere
        #  CLONE_USER_AT_HOST=\'$CLONE_USER\'@\'$WSREP_SST_OPT_HOST\'
        CLONE_USER_AT_HOST=\'$CLONE_USER\'

        # Use script files to avoid sensitive information on the command line
        CLONE_PREPARE_SQL=$(mktemp -p $WSREP_SST_OPT_DATA --suffix=.sql clone_prepare_XXXX)
        # TODO: last two grants may be unnecessary

cat << EOF > "$CLONE_PREPARE_SQL"
CREATE USER $CLONE_USER_AT_HOST IDENTIFIED BY '$CLONE_PSWD' $REQUIRE_SSL;
GRANT BACKUP_ADMIN ON *.* TO $CLONE_USER_AT_HOST;
GRANT SELECT ON performance_schema.* TO $CLONE_USER_AT_HOST;
GRANT EXECUTE ON *.* TO $CLONE_USER_AT_HOST;
EOF
        RC=0
        $MYSQL_ACLIENT < $CLONE_PREPARE_SQL || RC=$?

        if [ "$RC" -ne 0 ]
        then
            wsrep_log_error "Donor prepare returned code $RC"
            cat $CLONE_PREPARE_SQL >> /dev/stderr
            exit $RC
        fi

        if [ -z $PROGRESS ]
        then
            # Clone command below blocks until the end of SST, so start
            # monitoring thread now
            progress_monitor_setup "$MYSQL_CLIENT" "$ADMIN_USER" \
                "$WSREP_SST_OPT_SOCKET"
            progress_monitor_run "$MYSQL_CLIENT" "$ADMIN_USER" \
                "$WSREP_SST_OPT_SOCKET" &
            readonly progress_monitor=$!
        else
            readonly progress_monitor=""
        fi

        export MYSQL_PWD="$RECIPIENT_PSWD"
        # Find own address (from which we connected)
        USER=`$MYSQL_RCLIENT --skip-column-names -e 'SELECT USER()'`
        LHOST=${USER##*@}
        DONOR=$LHOST:$WSREP_SST_OPT_LPORT

        # Use script file to avoid sensitive information on the command line
        CLONE_EXECUTE_SQL=$(mktemp -p $WSREP_SST_OPT_DATA --suffix=.sql clone_execute_XXXX)
cat << EOF > "$CLONE_EXECUTE_SQL"
SET GLOBAL clone_valid_donor_list = '$DONOR';
CLONE INSTANCE FROM '$CLONE_USER'@'$LHOST':$WSREP_SST_OPT_LPORT
IDENTIFIED BY '$CLONE_PSWD' $REQUIRE_SSL;
EOF

        # Acrual cloning process
        $MYSQL_RCLIENT $CLIENT_SSL_OPTIONS < $CLONE_EXECUTE_SQL || RC=$?

        if [ -n progress_monitor ]
        then
            # Make sure monitoring thread dies
            kill -s SIGTERM $progress_monitor || :
            wait $progress_monitor || :  # ignore progress_monitor exit code
        fi

        if [ "$RC" -ne 0 ]
        then
            wsrep_log_error "Clone command returned code $RC"
            cat $CLONE_EXECUTE_SQL >> /dev/stderr
            # Attempt to shutdown recipient daemon
            eval $MYSQL_RCLIENT -e "SHUTDOWN"
            wsrep_log_info "Recipient shutdown: $?"
            case $RC in
            *)  RC=255 # unknown error
                ;;
            esac
            exit $RC
        fi

    else # BYPASS
        wsrep_log_info "Bypassing state dump."

        # Instruct recipient to shutdown
        export MYSQL_PWD="$RECIPIENT_PSWD"
        eval $MYSQL_RCLIENT -e "SHUTDOWN"
    fi

    echo "done $WSREP_SST_OPT_GTID"

elif [ "$WSREP_SST_OPT_ROLE" = "joiner" ]
then
    CLEANUP_FILES=

    wsrep_check_programs grep
    wsrep_check_programs ps

    if test -z "$WSREP_SST_OPT_DATA";   then wsrep_log_error "DATA cannot be nil";   exit $EINVAL; fi
    if test -z "$WSREP_SST_OPT_PARENT"; then wsrep_log_error "PARENT cannot be nil"; exit $EINVAL; fi

    #
    #  Find binary to run
    #
    PARENT_PROC=(`ps -hup $WSREP_SST_OPT_PARENT`)
    CLONE_BINARY=${PARENT_PROC[10]}
    if [ ! -x "$CLONE_BINARY" ]
    then
        basedir=$(dirname $(dirname $0))
        if [ -n "$basedir" -a -x "$basedir/$CLONE_BINARY" ]
        then
            CLONE_BINARY="$basedir/$CLONE_BINARY"
        else
            wsrep_log_error "Could not determine binary to run: $CLONE_BINARY"
            exit $EINVAL
        fi
    fi

    #
    # Find where libs needed for the binary are
    #
    CLONE_LIBS="$WSREP_SST_OPT_PLUGIN_DIR"

    # 1. Try plugins dir
    if [ -z "$CLONE_LIBS" ] && $MY_PRINT_DEFAULTS "mysqld" | grep -q plugin[_-]dir
    then
        CLONE_LIBS=$($MY_PRINT_DEFAULTS mysqld | grep -- "--plugin[_-]dir" | cut -d= -f2)
        # scale up to the first "mysql" occurence
        while [ ${#CLONE_LIBS} -gt "1" ]
        do
            [ `basename $CLONE_LIBS` = "mysql" ] && break
            CLONE_LIBS=$(dirname $CLONE_LIBS)
        done
        if ls $CLONE_LIBS/private/lib* > /dev/null 2>&1
        then
            CLONE_LIBS="$CLONE_LIBS/private"
        else
            wsrep_log_info "Could not find private libs in '$CLONE_LIBS' from plugin_dir: $($MY_PRINT_DEFAULTS mysqld | grep -- '--plugin_dir' | cut -d= -f2)"
            CLONE_LIBS=
        fi
    fi

    # 2. Try binary path
    if [ -z "$CLONE_LIBS" ]
    then
        CLONE_LIBS=$(dirname $(dirname $CLONE_BINARY))
        if ls $CLONE_LIBS/lib64/mysql/private/lib* > /dev/null 2>&1
        then
            CLONE_LIBS="$CLONE_LIBS/lib64/mysql/private/"
        elif ls $CLONE_LIBS/lib/mysql/private/lib* > /dev/null 2>&1
        then
            CLONE_LIBS="$CLONE_LIBS/lib/mysql/private/"
        else
            wsrep_log_info "Could not find private libs by binary name: $(dirname $(dirname $CLONE_BINARY))"
            CLONE_LIBS=
        fi
    fi

    # 3. Try this script path
    if [ -z "$CLONE_LIBS" ]
    then
        CLONE_LIBS=$(dirname $(dirname $0))
        if ls $CLONE_LIBS/lib64/mysql/private/lib* > /dev/null 2>&1
        then
            CLONE_LIBS="$CLONE_LIBS/lib64/mysql/private"
        elif ls $CLONE_LIBS/lib/mysql/private/lib* > /dev/null 2>&1
        then
            CLONE_LIBS="$CLONE_LIBS/lib/mysql/private"
        else
            wsrep_log_info "Could not find private libs by script path: $(dirname $(dirname $0))"
            CLONE_LIBS=
        fi
    fi

    if [ -d "$CLONE_LIBS" ]
    then
        CLONE_ENV="LD_LIBRARY_PATH=$CLONE_LIBS:${LD_LIBRARY_PATH:-}"
    else
        wsrep_log_info "Could not determine private library path for mysqld: $CLONE_LIBS. Leaving LD_LIBRARY_PATH unmodified."
        CLONE_ENV=""
    fi

    MODULE="clone_sst"
    CLONE_SOCK_DIR=`mktemp --tmpdir -d ${MODULE}_XXXXXX`

    CLONE_PID_FILE="$WSREP_SST_OPT_DATA/$MODULE.pid"

    if check_pid_file $CLONE_PID_FILE
    then
        CLONE_PID=`cat $CLONE_PID_FILE`
        wsrep_log_error "cloning daemon already running (PID: $CLONE_PID)"
        exit 114 # EALREADY
    fi
    rm -rf "$CLONE_PID_FILE"

    CLONE_PORT=${WSREP_SST_OPT_PORT:-3306}
    CLONE_ADDR=$WSREP_SST_OPT_HOST:$CLONE_PORT

    CLONE_ERR="$WSREP_SST_OPT_DATA/$MODULE.err"
    CLONE_SQL="$WSREP_SST_OPT_DATA/$MODULE.sql"

    CLONE_SOCK="$CLONE_SOCK_DIR/cmysql.sock"
    CLONE_X_SOCK="$CLONE_SOCK_DIR/cmysqlx.sock"
    if [ ${#CLONE_X_SOCK} -ge 104 ]
    then
        wsrep_log_error "Unix socket path name length for CLONE SST receiver"\
                        "'$CLONE_X_SOCK' is greater than commonly acceptable"\
                        "limit of 104 bytes."
                        # Linux: 108, FreeBSD: 104
        exit $EINVAL
    fi

    [ -z "$WSREP_SST_OPT_CONF" ] \
    && DEFAULTS_FILE_OPTION="" \
    || DEFAULTS_FILE_OPTION="--defaults-file='$WSREP_SST_OPT_CONF'"

    DEFAULT_OPTIONS=" \
     $DEFAULTS_FILE_OPTION \
     --datadir='$WSREP_SST_OPT_DATA' \
     --wsrep_provider=none \
     --wsrep_status_file='' \
     --secure_file_priv='' \
     --socket="$CLONE_SOCK" \
     --mysqlx_socket="$CLONE_X_SOCK" \
     --log_error='$CLONE_ERR' \
     --pid_file='$CLONE_PID_FILE' \
     --plugin-dir='$CLONE_LIBS' \
    "
    SKIP_NETWORKING_OPTIONS=" \
     --skip-networking \
     --skip-mysqlx \
    "

    if [ ! -d "$WSREP_SST_OPT_DATA/mysql" -o \
         ! -r "$WSREP_SST_OPT_DATA/mysql.ibd" ]
    then
        # No data dir, need to initialize one first, to make connections to
        # this node possible
        wsrep_log_info "Initializing data directory at $WSREP_SST_OPT_DATA"
        eval $CLONE_ENV $CLONE_BINARY $DEFAULT_OPTIONS \
             $SKIP_NETWORKING_OPTIONS
             --initialize || \
        ( wsrep_log_error "Failed to initialize data directory. Some hints below:"
          grep '[ERROR]' $CLONE_ERR | cut -d ']' -f 4- | while read msg
          do
              wsrep_log_error "> $msg"
          done
          wsrep_log_error "Full log at $CLONE_ERR"
          exit 1 )
    fi

    CLONE_USER="clone_sst_$(date +%y%m%d_%H%M%S)"
    CLONE_PSWD=`wsrep_gen_secret`"$PSWD_POLICY"

# Need to create an extra user for 'localhost' because in some installations
# by default exists user ''@'localhost' and shadows every user with '%'.
cat << EOF > "$CLONE_SQL"
CREATE USER '$CLONE_USER'@'%' IDENTIFIED WITH mysql_native_password BY '$CLONE_PSWD';
GRANT SYSTEM_VARIABLES_ADMIN ON *.* TO '$CLONE_USER';
GRANT CLONE_ADMIN ON *.* TO '$CLONE_USER';
GRANT SUPER ON *.* to '$CLONE_USER';
GRANT SHUTDOWN ON *.* TO '$CLONE_USER';
GRANT EXECUTE ON *.* to '$CLONE_USER';
GRANT INSERT ON mysql.plugin to '$CLONE_USER';
GRANT SELECT ON performance_schema.* TO '$CLONE_USER';
GRANT UPDATE ON performance_schema.setup_instruments TO '$CLONE_USER';
GRANT UPDATE ON performance_schema.setup_consumers TO '$CLONE_USER';
CREATE USER '$CLONE_USER'@'localhost' IDENTIFIED WITH mysql_native_password BY '$CLONE_PSWD';
GRANT SYSTEM_VARIABLES_ADMIN ON *.* TO '$CLONE_USER'@'localhost';
GRANT CLONE_ADMIN ON *.* TO '$CLONE_USER'@'localhost';
GRANT SUPER ON *.* to '$CLONE_USER'@'localhost';
GRANT SHUTDOWN ON *.* TO '$CLONE_USER'@'localhost';
GRANT EXECUTE ON *.* to '$CLONE_USER'@'localhost';
GRANT INSERT ON mysql.plugin to '$CLONE_USER'@'localhost';
GRANT SELECT ON performance_schema.* TO '$CLONE_USER'@'localhost';
GRANT UPDATE ON performance_schema.setup_instruments TO '$CLONE_USER'@'localhost';
GRANT UPDATE ON performance_schema.setup_consumers TO '$CLONE_USER'@'localhost';
EOF

    wsrep_log_info "Launching clone recipient daemon"
    eval MYSQLD_PARENT_PID=1 $CLONE_ENV $CLONE_BINARY $DEFAULT_OPTIONS \
        --port="$CLONE_PORT" \
        --init_file="$CLONE_SQL" \
        --daemonize

    # wait for the receiver process to start
    until [ -n "$(cat $CLONE_PID_FILE)" ]
    do
        sleep 0.2
    done
    CLONE_REAL_PID=`cat $CLONE_PID_FILE`

    trap "exit 32" HUP PIPE
    trap "exit 3"  INT TERM ABRT
    trap cleanup_joiner EXIT

    export MYSQL_PWD=$CLONE_PSWD

    wsrep_log_info "Waiting for clone recipient daemon to be ready for connections at port $CLONE_PORT"
    to_wait=60 # 1 minute
    until $MYSQL_CLIENT -u$CLONE_USER -h$SST_HOST_STRIPPED -P$CLONE_PORT \
          -e 'SELECT USER()' > /dev/null
    do
        if [ $to_wait -eq 0 ]
        then
            wsrep_log_error "Timeout waiting for clone recipient daemon"
            exit $ETIMEDOUT
        fi
        to_wait=$(( $to_wait - 1 ))
        sleep 1
    done

    MYSQL_ACLIENT="$MYSQL_CLIENT -u$CLONE_USER -S$CLONE_SOCK \
                   --batch --skip_column_names --silent"
    setup_clone_plugin "recipient"

    # Report clone credentials/address to the caller
    echo "ready $CLONE_USER:$CLONE_PSWD@$WSREP_SST_OPT_HOST:$CLONE_PORT"

    if [ -z "$PROGRESS" ]
    then
        progress_monitor_setup "$MYSQL_CLIENT" "$CLONE_USER" "$CLONE_SOCK"
        progress_monitor_run "$MYSQL_CLIENT" "$CLONE_USER" "$CLONE_SOCK" &
        readonly progress_monitor=$!
    else
        readonly progress_monitor=""
    fi

    wsrep_log_info "Waiting for clone recipient daemon to finish"
    while check_pid_file "$CLONE_PID_FILE"
    do
        wsrep_log_debug "Checking parent: $WSREP_SST_OPT_PARENT"
        check_parent $WSREP_SST_OPT_PARENT
        sleep 0.2
    done
    CLONE_REAL_PID=0

    if [ -n "$progress_monitor" ]
    then
        # Make sure monitoring thread dies
        kill -s SIGTERM $progress_monitor || :
        wait $progress_monitor || :  # ignore progress_monitor exit code
    fi

    wsrep_log_info "Performing data recovery"
    eval $CLONE_ENV $CLONE_BINARY $DEFAULT_OPTIONS $SKIP_NETWORKING_OPTIONS \
         --wsrep_recover
    RP="$(grep '\[WSREP\] Recovered position:' $CLONE_ERR || :)"

    # Remove created clone user
cat << EOF > "$CLONE_SQL"
DROP USER IF EXISTS $CLONE_USER;
DROP USER IF EXISTS $CLONE_USER@'localhost';
SHUTDOWN;
EOF
    eval $CLONE_ENV $CLONE_BINARY $DEFAULT_OPTIONS $SKIP_NETWORKING_OPTIONS \
         --init-file="$CLONE_SQL"

    if [ -z "$RP" ]
    then
        echo "Failed to recover position from $CLONE_ERR";
    else
        echo $RP | sed 's/.*WSREP\]\ Recovered\ position://' | sed 's/^[ \t]*//'
    fi

    CLEANUP_FILES=1
else
    wsrep_log_error "Unrecognized role: '$WSREP_SST_OPT_ROLE'"
    exit 22 # EINVAL
fi

exit 0
