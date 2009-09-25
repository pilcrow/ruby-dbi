#
# See DBI::BaseStatement, and DBI::DBD::Pg::Tuples.
#
#--
# Peculiar Statement responsibilities:
#  - Translate dbi params (?, ?, ...) to Pg params ($1, $2, ...)
#  - Translate DBI::Binary objects to Pg large objects (lo_*)
#  - Track underlying "prepared query plans"
#
#    Note that we refer to PostgreSQL's prepared statements as
#    "plans" (borrowed from the PostgreSQL manual's statement syntax
#    for PREPARE/DEALLOCATE commands) to disambiguate between these
#    underlying constructs from the DBD::Pg::Statement and a typical
#    DBI::StatementHandle.

class DBI::DBD::Pg::Statement < DBI::BaseStatement

    PG_PLAN_NAME_PREFIX = 'ruby-dbi:Pg:'

    def initialize(db, sql)
        super(db)
        @db  = db
        @sql = sql
        @plan_name = PG_PLAN_NAME_PREFIX + self.object_id.to_s
        @result = nil
        @bindvars = []

        # Clean up nicely if someone forgot to finish() a previous but
        # now finalized Pg::Statement which happened to have our same
        # object_id, and so our same @plan_name.
        # See [bug rf-27113]
        internal_safe_deallocate()
    end

    def bind_param(index, value, options)
        @bindvars[index-1] = value
    end

    #
    # See DBI::BaseDatabase#execute.
    #
    def execute
        # We presently have to do the DBI::Binary -> BLOB conversion ourself.
        # See DBI::DBD::Pg::Database#_convert_if_binary
        @bindvars.collect! do |param|
            @db._convert_if_binary(param)
        end
 
        internal_prepare

        if not @db['AutoCommit'] then
            #          if not SQL.query?(boundsql) and not @db['AutoCommit'] then
            @db.start_transaction unless @db.in_transaction?
        end

        pg_result = @db._exec_prepared(@plan_name, *@bindvars)

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

    #
    # pg result:  Release resources, if any
    # pg plan:    DEALLOCATE handle, if PREPAREd
    #
    def finish
        @result.finish if @result
        internal_safe_deallocate

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
    def internal_safe_deallocate
        if @db.prepared_plans.include?(@plan_name)
            @db._exec("DEALLOCATE \"#{@plan_name}\"")
            @db.prepared_plans.delete(@plan_name)
        end
    rescue PGError
        raise DBI::InternalError("internal DEALLOCATE #@plan_name failed")
    end

    # prepare the statement at a lower level.
    def internal_prepare
        return if @db.prepared_plans.include?(@plan_name)

        pg_parameters = self.class.translate_param_markers(@sql)
        @db._prepare(@plan_name, pg_parameters)

        @db.prepared_plans.add(@plan_name)
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
