module DBI::DBD::Mysql
    #
    # Models the DBI::BaseDatabase API to create DBI::DatabaseHandle objects.
    # 
    class Database < DBI::BaseDatabase
        include Util

        #
        # Hash to translate MySQL type names to DBI SQL type constants
        #
        # Only used in #mysql_type_info.
        #
        #--
        # Eli Green:
        #   The hope is that we don't ever need to just assume the default values.
        #   However, in some cases (notably floats and doubles), I have seen
        #   "show fields from table" return absolutely zero information about size
        #   and precision. Sigh. I probably should have made a struct to store
        #   this info in ... but I didn't.
        #++
        MYSQL_to_XOPEN = {
                    "TINYINT"    => [DBI::SQL_TINYINT, 1, nil],
                    "SMALLINT"   => [DBI::SQL_SMALLINT, 6, nil],
                    "MEDIUMINT"  => [DBI::SQL_SMALLINT, 6, nil],
                    "INT"        => [DBI::SQL_INTEGER, 11, nil],
                    "INTEGER"    => [DBI::SQL_INTEGER, 11, nil],
                    "BIGINT"     => [DBI::SQL_BIGINT, 25, nil],
                    "INT24"      => [DBI::SQL_BIGINT, 25, nil],
                    "REAL"       => [DBI::SQL_REAL, 12, nil],
                    "FLOAT"      => [DBI::SQL_FLOAT, 12, nil],
                    "DECIMAL"    => [DBI::SQL_DECIMAL, 12, nil],
                    "NUMERIC"    => [DBI::SQL_NUMERIC, 12, nil],
                    "DOUBLE"     => [DBI::SQL_DOUBLE, 22, nil],
                    "CHAR"       => [DBI::SQL_CHAR, 1, nil],
                    "VARCHAR"    => [DBI::SQL_VARCHAR, 255, nil],
                    "DATE"       => [DBI::SQL_DATE, 10, nil],
                    "TIME"       => [DBI::SQL_TIME, 8, nil],
                    "TIMESTAMP"  => [DBI::SQL_TIMESTAMP, 19, nil],
                    "DATETIME"   => [DBI::SQL_TIMESTAMP, 19, nil],
                    "TINYBLOB"   => [DBI::SQL_BINARY, 255, nil],
                    "BLOB"       => [DBI::SQL_VARBINARY, 65535, nil],
                    "MEDIUMBLOB" => [DBI::SQL_VARBINARY, 16277215, nil],
                    "LONGBLOB"   => [DBI::SQL_LONGVARBINARY, 2147483657, nil],
                    "TINYTEXT"   => [DBI::SQL_VARCHAR, 255, nil],
                    "TEXT"       => [DBI::SQL_LONGVARCHAR, 65535, nil],
                    "MEDIUMTEXT" => [DBI::SQL_LONGVARCHAR, 16277215, nil],
                    "LONGTEXT"   => [DBI::SQL_LONGVARCHAR, 2147483657, nil],
                    "ENUM"       => [DBI::SQL_CHAR, 255, nil],
                    "SET"        => [DBI::SQL_CHAR, 255, nil],
                    "BIT"        => [DBI::SQL_BIT, 8, nil],
                    nil          => [DBI::SQL_OTHER, nil, nil]
        }


        # 
        # This maps type names to DBI Types.
        #
        TYPE_MAP = {}

        ::Mysql::Field.constants.grep(/^TYPE_/).each do |sym|
            mysql_type = MysqlField.const_get(sym)  # numeric type code
            coercion_method = DBI::Type::Varchar    # default coercion method
            case sym.to_s
            when 'TYPE_TINY'
                mysql_type_name = 'TINYINT'
                coercion_method = DBI::Type::Integer
            when 'TYPE_SHORT'
                mysql_type_name = 'SMALLINT'
                coercion_method = DBI::Type::Integer
            when 'TYPE_INT24'
                mysql_type_name = 'MEDIUMINT'
                coercion_method = DBI::Type::Integer
            when 'TYPE_LONG'
                mysql_type_name = 'INT'
                coercion_method = DBI::Type::Integer
            when 'TYPE_LONGLONG'
                mysql_type_name = 'BIGINT'
                coercion_method = DBI::Type::Integer
            when 'TYPE_FLOAT'
                mysql_type_name = 'FLOAT'
                coercion_method = DBI::Type::Float
            when 'TYPE_DOUBLE'
                mysql_type_name = 'DOUBLE'
                coercion_method = DBI::Type::Float
            when 'TYPE_VAR_STRING', 'TYPE_STRING'
                mysql_type_name = 'VARCHAR'    # questionable?
                coercion_method = DBI::Type::Varchar
            when 'TYPE_DATE'
                mysql_type_name = 'DATE'
                coercion_method = DBI::DBD::Mysql::Type::Date
            when 'TYPE_TIME'
                mysql_type_name = 'TIME'
                coercion_method = DBI::Type::Timestamp
            when 'TYPE_DATETIME', 'TYPE_TIMESTAMP'
                mysql_type_name = 'DATETIME'
                coercion_method = DBI::Type::Timestamp
            when 'TYPE_CHAR'
                mysql_type_name = 'TINYINT'    # questionable?
            when 'TYPE_TINY_BLOB'
                mysql_type_name = 'TINYBLOB'   # questionable?
            when 'TYPE_MEDIUM_BLOB'
                mysql_type_name = 'MEDIUMBLOB' # questionable?
            when 'TYPE_LONG_BLOB'
                mysql_type_name = 'LONGBLOB'   # questionable?
            when 'TYPE_GEOMETRY'
                mysql_type_name = 'BLOB'       # questionable?
            when 'TYPE_YEAR',
                 'TYPE_DECIMAL',                                     # questionable?
                 'TYPE_BLOB',                                        # questionable?
                 'TYPE_ENUM',
                 'TYPE_SET',
                 'TYPE_BIT',
                 'TYPE_NULL'
                mysql_type_name = sym.to_s.sub(/^TYPE_/, '')
            else
                mysql_type_name = 'UNKNOWN'
            end
            TYPE_MAP[mysql_type] = [mysql_type_name, coercion_method]
        end
        TYPE_MAP[nil] = ['UNKNOWN', DBI::Type::Varchar]
        TYPE_MAP[246] = ['NUMERIC', DBI::Type::Decimal]

        #
        # Constructor. Attributes supported:
        #
        # * AutoCommit: Commit after each executed statement. This will raise
        #   a DBI::NotSupportedError if the backend does not support
        #   transactions.
        #
        def initialize(handle, attr)
            super
            # check server version to determine transaction capability
            ver_str = @handle.get_server_info
            major, minor, teeny = ver_str.split(".")
            teeny.sub!(/\D*$/, "")  # strip any non-numeric suffix if present
            server_version = major.to_i*10000 + minor.to_i*100 + teeny.to_i
            # It's not until 3.23.17 that SET AUTOCOMMIT,
            # BEGIN, COMMIT, and ROLLBACK all are available
            stub_out_transaction_support if server_version < 32317
            # assume that the connection begins in AutoCommit mode
            @attr['AutoCommit'] = true
        end

        def _connection # :nodoc:
            # for use by My::Stmt
            return @handle
        end

        def disconnect
            self.rollback unless @attr['AutoCommit']
            @handle.close
        rescue MyError => err
            error(err)
        end

        def database_name
            DBI::DatabaseHandle.new(self).select_one("SELECT DATABASE()")[0]
        rescue MyError => err
            error(err)
        end

        def ping
            @handle.ping
            true
        rescue MyError
            false
        end

        def tables
            @handle.list_tables
        rescue MyError => err
            error(err)
        end

        #
        # See DBI::BaseDatabase#columns.
        #
        # Extra attributes:
        #
        # * sql_type: XOPEN integer constant relating to type.
        # * nullable: true if the column allows NULL as a value.
        # * indexed: true if the column belongs to an index.
        # * primary: true if the column is a part of a primary key.
        # * unique: true if the values in this column are unique.
        # * default: the default value if this column is not explicitly set. 
        #
        def columns(table)
            dbh = DBI::DatabaseHandle.new(self)
            uniques = []
            dbh.execute("SHOW INDEX FROM #{table}") do |sth|
                sth.each do |row|
                    uniques << row[4] if row[1] == 0
                end
            end  

            ret = nil
            dbh.execute("SHOW FIELDS FROM #{table}") do |sth|
                ret = sth.collect do |row|
                    name, type, nullable, key, default, extra = row
                    #type = row[1]
                    #size = type[type.index('(')+1..type.index(')')-1]
                    #size = 0
                    #type = type[0..type.index('(')-1]

                    sqltype, type, size, decimal = mysql_type_info(row[1])
                    col = Hash.new
                    col['name']           = name
                    col['sql_type']       = sqltype
                    col['type_name']      = type
                    col['nullable']       = nullable == "YES"
                    col['indexed']        = key != ""
                    col['primary']        = key == "PRI"
                    col['unique']         = uniques.index(name) != nil
                    col['precision']      = size
                    col['scale']          = decimal
                    col['default']        = default
                    # XXX col['mysql_extra'] = extra

                    case col['type_name']
                    when 'timestamp'
                        col['dbi_type'] = DBI::Type::Timestamp
                    end

                    col
                end # collect
            end # execute

            ret
        end

        def do(stmt, *bindvars)
            st = Statement.new(self, stmt)
            st.bind_params(*bindvars)
            res = st.execute
            st.finish
            return res
        rescue MyError => err
            error(err)
        end


        def prepare(statement)
            Statement.new(self, statement)
        end

        #
        # MySQL has several backend "storage engines," not all of which
        # support transactions.  See #rollback() for one strategy if your
        # code may be dealing with such an engine.
        # 
        # If your version of MySQL is too old to support transactions at
        # all (< 3.23.17), this method is replaced by a stub which raises
        # a DBI::NotSupportedError.
        #
        def commit
          self.do('COMMIT') # already traps MyError
        end

        #
        # Some MySQL storage engines (physical table types), such as MyISAM,
        # do not support transactions.  In this case, calling #rollback()
        # will succeed, even though operations against the transactionless
        # tables will not be rolled back.
        #
        # MySQL communicates this situation as a driver-level warning, which
        # you might check for like so:
        #
        #   dbh.rollback
        #   if dbh.func(:warning_count) > 0
        #     warnings = dbh.select_all('SHOW WARNINGS')
        #     rollback_actually_failed = warnings.find { |row|
        #       row[1] == ::MysqlError::ER_WARNING_NOT_COMPLETE_ROLLBACK
        #     }
        #     # Er, now what?
        #   end
        #
        # Note that doing so requires intimacy with the peculiars of your
        # driver and server version, which is contrary to the spirit of the
        # DBI.
        #
        # If your version of MySQL is too old to support transactions at
        # all (< 3.23.17), this method is replaced by a stub which raises
        # a DBI::NotSupportedError.
        #
        def rollback
            self.do('ROLLBACK') # already traps MyError
        end


#                 def quote(value)
#                     case value
#                     when String
#                       "'#{@handle.quote(value)}'"
#                     when DBI::Binary
#                       "'#{@handle.quote(value.to_s)}'"
#                     when TrueClass
#                       "'1'"
#                     when FalseClass
#                       "'0'"
#                     else
#                         super
#                     end
#                 end

        #
        # See DBI::DBD::MySQL::Database.new for supported attributes and usage.
        #
        def []=(attr, value)
            case attr
            when 'AutoCommit'
                self.do("SET AUTOCOMMIT=" + (value ? "1" : "0"))
            end

            @attr[attr] = value
        end

        private # -------------------------------------------------

        #
        # Given a type name, weans some basic information from that and returns
        # it in a format similar to columns.
        #
        # Return is an array of +sqltype+, +type+, +size+, and +decimal+.
        # +sqltype+ is the XOPEN type, and +type+ is the string with the
        # parameters removed.
        #
        # +size+ and +decimal+ refer to +precision+ and +scale+ in most cases,
        # but not always for all types. Please consult the documentation for
        # your MySQL version.
        #
        #
        def mysql_type_info(typedef)
            sqltype, type, size, decimal = nil, nil, nil, nil

            pos = typedef.index('(')
            if not pos.nil?
                type = typedef[0..pos-1]
                size = typedef[pos+1..-2]
                pos = size.index(',')
                if not pos.nil?
                    size, decimal = size.split(',', 2)
                    decimal = decimal.to_i
                end
                size = size.to_i
            else
                type = typedef
            end

            type_info = MYSQL_to_XOPEN[type.upcase] || MYSQL_to_XOPEN[nil]
            sqltype = type_info[0]
            if size.nil? then size = type_info[1] end
            if decimal.nil? then decimal = type_info[2] end
            return sqltype, type, size, decimal
        end

        #
        # Called by #initialize() if server version is too old to support
        # transactions.
        #
        def stub_out_transaction_support
            class << self
                def commit;   raise DBI::NotSupportedError end
                def rollback; raise DBI::NotSupportedError end
                def []=(key, value)
                    if key == 'AutoCommit' and !value
                        raise DBI::NotSupportedError
                    end
                    super
                end
            end
        end

        #--
        # Driver-specific functions ------------------------------------------------
        #++

        public

        def __createdb(db)
            @handle.create_db(db)
        rescue MyError => err
            error(err)
        end

        def __dropdb(db)
            @handle.drop_db(db)
        rescue MyError => err
            error(err)
        end

        def __shutdown
            @handle.shutdown
        rescue MyError => err
            error(err)
        end

        def __reload
            @handle.reload
        rescue MyError => err
            error(err)
        end

        def __insert_id
            @handle.insert_id
        rescue MyError => err
            error(err)
        end

        def __thread_id
            @handle.thread_id
        rescue MyError => err
            error(err)
        end

        def __info
            @handle.info
        rescue MyError => err
            error(err)
        end

        def __host_info
            @handle.host_info
        rescue MyError => err
            error(err)
        end

        def __proto_info
            @handle.proto_info
        rescue MyError => err
            error(err)
        end

        def __server_info
            @handle.server_info
        rescue MyError => err
            error(err)
        end

        def __client_info
            @handle.client_info
        rescue MyError => err
            error(err)
        end

        def __client_version
            @handle.client_version
        rescue MyError => err
            error(err)
        end

        def __stat
            @handle.stat
        rescue MyError => err
            error(err)
        end

        def __warning_count
            @handle.warning_count
        rescue MyError => err
            error(err)
        end

        def __sqlstate
            @handle.sqlstate
        rescue MyError => err
            error(err)
        end

    end # class Database
end
