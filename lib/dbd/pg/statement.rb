#
# See DBI::BaseStatement, and DBI::DBD::Pg::Tuples.
#
#--
# Peculiar Statement responsibilities:
#  - Translate dbi params (?, ?, ...) to Pg params ($1, $2, ...)
#  - Translate DBI::Binary objects to Pg large objects (lo_*)

class DBI::DBD::Pg::Statement < DBI::BaseStatement

    PG_STMT_NAME_PREFIX = 'ruby-dbi:Pg:'

    def initialize(db, sql)
        super(db)
        @db  = db
        @sql = sql
        @stmt_name = PG_STMT_NAME_PREFIX + self.object_id.to_s
        @result = nil
        @bindvars = []
        @prepared = false
    rescue PGError => err
        raise DBI::ProgrammingError.new(err.message)
    end

    def bind_param(index, value, options)
        @bindvars[index-1] = value
    end

    #
    # See DBI::BaseDatabase#execute.
    #
    def execute
        # We presently have to do the DBI::Binary -> BLOB conversion ourself.
        # See DBI::DBD::Pg::Database#convert_if_binary
        @bindvars.collect! do |param|
            @db._convert_if_binary(param)
        end
 
        internal_prepare

        if not @db['AutoCommit'] then
            #          if not SQL.query?(boundsql) and not @db['AutoCommit'] then
            @db.start_transaction unless @db.in_transaction?
        end

        pg_result = @db._exec_prepared(@stmt_name, *@bindvars)

        @result = DBI::DBD::Pg::Tuples.new(@db, pg_result)
    rescue PGError, RuntimeError => err
        raise DBI::ProgrammingError.new(err.message)
    end

    def fetch
        @result.fetchrow
    end

    def fetch_scroll(direction, offset)
        @result.fetch_scroll(direction, offset)
    end

    def finish
        internal_finish
        @result = nil
        @db = nil
    end

    #
    # See DBI::DBD::Pg::Tuples#column_info.
    #
    def column_info
        @result.column_info
    end

    def rows
        if @result
            @result.rows_affected
        else
            nil
        end
    end

    #
    # Attributes:
    # 
    # If +pg_row_count+ is requested and the statement has already executed,
    # postgres will return what it believes is the row count.
    #
    def [](attr)
        case attr
        when 'pg_row_count'
            if @result
                @result.row_count
            else
                nil
            end
        else
            @attr[attr]
        end
    end

    private 

    # finish the statement at a lower level
    def internal_finish
        @result.finish if @result
        @db._exec("DEALLOCATE \"#{@stmt_name}\"") if @prepared rescue nil
    end

    # prepare the statement at a lower level.
    def internal_prepare
        return if @prepared

        @stmt = @db._prepare(@stmt_name, self.class.translate_param_markers(@sql))
        @prepared = true
    end

    # -- class methods --

    # Prepare the given SQL statement, returning its PostgreSQL string
    # handle.  ?-style parameters are translated to $1, $2, etc.
    #--
    # TESTME  do ?::TYPE qualifers work?
    # FIXME:  DBI ought to supply a generic param converter, e.g.:
    #         sql = DBI::Utils::convert_placeholders(sql) do |i|
    #                 '$' + i.to_s
    #               end
    def self.translate_param_markers(sql)
        translator = DBI::SQL::PreparedStatement.new(DBI::DBD::Pg, sql)
        if translator.unbound.size > 0
            arr = (1..(translator.unbound.size)).collect{|i| "$#{i}"}
            sql = translator.bind( arr )
        end
        sql
    end
end # Statement
