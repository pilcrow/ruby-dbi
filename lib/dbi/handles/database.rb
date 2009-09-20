module DBI
    # DatabaseHandle is the interface the consumer sees after connecting to the
    # database via DBI.connect.
    #
    # It is strongly discouraged that DBDs inherit from this class directly;
    # please inherit from the DBI::BaseDatabase instead.
    #
    # Note: almost all methods in this class will raise InterfaceError if the
    # database is not connected.
    class DatabaseHandle < Handle

        attr_accessor :last_statement
        attr_accessor :raise_error

        # This is the driver name as supplied by the DBD's driver_name method.
        # Its primary utility is in DBI::TypeUtil#convert.
        def driver_name
            return @driver_name.dup if @driver_name
            return nil
        end

        # Assign the driver name. This can be leveraged to create custom type
        # management via DBI::TypeUtil#convert.
        def driver_name=(name)
            @driver_name = name
            @driver_name.freeze
        end

        #
        # Boolean if we are still connected to the database. See #ping.
        #
        def connected?
            @handle != dummy_handle
        end

        #
        # Disconnect from the database. Will raise InterfaceError if this was
        # already done prior.
        #
        def disconnect
            @handle.disconnect
            @handle = dummy_handle
        end

        #
        # Prepare a StatementHandle and return it. If given a block, it will
        # supply that StatementHandle as the first argument to the block, and
        # BaseStatement#finish it when the block is done executing.
        #
        def prepare(stmt)
            # One of two sanity_check calls, the other in #do()
            StatementHandle::sanity_check(stmt)
            @last_statement = stmt

            sth = StatementHandle.new(@handle.prepare(stmt), @convert_types)
            # FIXME trace sth.trace(@trace_mode, @trace_output)
            sth.dbh = self
            sth.raise_error = raise_error

            return sth unless block_given?

            begin
              yield sth
            ensure
              sth.finish unless sth.finished?
            end
        end

        #
        # Prepare and execute a statement. It has block semantics equivalent
        # to #prepare.
        #
        def execute(stmt, *bindvars)
            # Changed 2009-09-19 (from 0.4.3) to be implemented in terms of
            # prepare/execute.  DBD::Driver::execute() is obsolete.
            # 
            if block_given?
                prepare(stmt) do |sth|
                    sth.execute(*bindvars) # StatementHandle does param conversion
                    yield sth
                end
            else
                sth = prepare(stmt)
                sth.execute()
                sth
            end 
        end

        #
        # Perform a statement. This goes straight to the DBD's implementation
        # of #do (and consequently, BaseDatabase#do), and does not work like
        # #execute and #prepare. Should return a row modified count.
        #
        def do(stmt, *bindvars)
            # We sanity_check and convert parameters ourselves, since we're
            # bypassing StatementHandle creation...
            StatementHandle::sanity_check(stmt)

            @last_statement = stmt
            @handle.do(stmt, *DBI::Utils::ConvParam.conv_param(driver_name, *bindvars))
        end

        #
        # Executes a statement and returns the first row from the result.
        #
        def select_one(stmt, *bindvars)
            row = nil
            execute(stmt, *bindvars) do |sth|
                row = sth.fetch 
            end
            row
        end

        #
        # Executes a statement and returns all rows from the result. If a block
        # is given, it is executed for each row.
        # 
        def select_all(stmt, *bindvars, &p)
            rows = nil
            execute(stmt, *bindvars) do |sth|
                if block_given?
                    sth.each(&p)
                else
                    rows = sth.fetch_all 
                end
            end
            return rows
        end

        #
        # Return the name of the database we are connected to.
        #
        def database_name
            @handle.database_name
        end

        #
        # Return the tables available to this DatabaseHandle as an array of strings.
        #
        def tables
            @handle.tables
        end

        #
        # Returns the columns of the provided table as an array of ColumnInfo
        # objects. See BaseDatabase#columns for the minimum parameters that
        # this method must provide.
        #
        def columns( table )
            @handle.columns( table ).collect {|col| ColumnInfo.new(col) }
        end

        #
        # Attempt to establish if the database is still connected. While
        # #connected? returns the state the DatabaseHandle thinks is true, this
        # is an active operation that will contact the database.
        #
        def ping
            @handle.ping
        end

        #
        # Attempt to escape the value, rendering it suitable for inclusion in a SQL statement.
        #
        def quote(value)
            @handle.quote(value)
        end

        #
        # Force a commit to the database immediately.
        #
        def commit
            @handle.commit
        end

        #
        # Force a rollback to the database immediately.
        #
        def rollback
            @handle.rollback
        end

        #
        # Commits, runs the block provided, yielding the DatabaseHandle as it's
        # argument. If an exception is raised through the block, rollback occurs.
        # Otherwise, commit occurs.
        #
        def transaction
            raise InterfaceError, "No block given" unless block_given?

            commit
            begin
                yield self
                commit
            rescue Exception
                rollback
                raise
            end
        end

        # Get an attribute from the DatabaseHandle.
        def [] (attr)
            @handle[attr]
        end

        # Set an attribute on the DatabaseHandle.
        def []= (attr, val)
            @handle[attr] = val
        end
    end #-- class DatabaseHandle
end
