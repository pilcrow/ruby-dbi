module DBI
    #
    # StatementHandle is the interface the consumer sees after successfully
    # issuing a DatabaseHandle#prepare. They may also be exposed through other
    # methods that send statements to the database.
    #
    # Almost all methods in this class will raise InterfaceError if the
    # statement is already finished.
    #
    class StatementHandle < Handle

        def self.sanity_check(stmt)
            raise DBI::InterfaceError, "Statement is empty, or contains nothing but whitespace" if stmt !~ /\S/
        end

        include Enumerable

        attr_accessor :dbh
        attr_accessor :raise_error

        def initialize(handle, convert_types=true)
            super(handle)
            clear_state!
            @convert_types = convert_types
        end

        # Returns true if the StatementHandle has had #finish called on it,
        # explicitly or otherwise.
        def finished?
            @handle == dummy_handle
        end

        # Returns true if the statement is believed to return data upon #fetch.
        #
        # The current reliability of this (and the concept in general) is
        # suspect.
        # Analogous to perl-DBI's stmt handle "Active" concept
        def fetchable?
            @fetchable
        end

        #
        # Instruct successive calls to #fetch to cast the type returned into
        # `type`, for row position `pos`. Like all bind_* calls, `pos` indexes
        # starting at 1.
        #
        # `type` is an object with the DBI::Type calling convention.
        #
        # This call must be called after #execute has successfully ran,
        # otherwise it will raise InterfaceError.
        #
        # Example:
        #  # `foo` is an integer and this statement will return two rows. 
        #  sth = dbh.prepare("select foo from bar") 
        #  # would raise InterfaceError if called here
        #  sth.execute
        #
        #  sth.bind_coltype(1, DBI::Type::Varchar) 
        #  # would normally use DBI::Type::Integer and return a Fixnum. We'll make it a string.
        #  sth.fetch => ["1"]
        #
        #  # Here we coerce it to Float.
        #  sth.bind_coltype(1, DBI::Type::Float)
        #  sth.fetch => [1.0]
        #  sth.finish
        #
        def bind_coltype(pos, type)
            @handle.raise_exception if finished?
            raise InterfaceError, "Cannot call bind_coltype before execute" unless @fetchable

            coltypes = column_types

            if (pos - 1) < 1
                raise InterfaceError, "bind positions index starting at 1"
            end

            coltypes[pos-1] = type
            @row = DBI::Row.new(column_names, coltypes, nil, @convert_types)
        end

        #
        # Just like BaseStatement#bind_param, but will attempt to convert the
        # type if it's supposed to, adhering to the DBD's current ruleset.
        #
        def bind_param(param, value, attribs=nil)
            if @convert_types
                value = DBI::Utils::ConvParam.conv_param(dbh.driver_name, value)[0]
            end

            @handle.bind_param(param, value, attribs)
        end


        # Execute the statement.
        #
        # This generally means that the statement will be sent to the database
        # and some form of result cursor will be obtained, but is ultimately
        # driver-dependent.
        #
        # If arguments are supplied, these are fed to #bind_param.
        def execute(*bindvars)
            cancel     # cancel before 

            if bindvars.size > 0
              if @convert_types
                bindvars = DBI::Utils::ConvParam.conv_param(dbh.driver_name, *bindvars)
              end

              @handle.bind_params(*bindvars)
            end
            @handle.execute
            @fetchable = true

            # TODO:?
            #if @row.nil?
            @row = DBI::Row.new(column_names, column_types, nil, @convert_types)
            #end
            return nil
        end

        #
        # Finish the statement, causing the database to release all assets
        # related to it (any result cursors, normally).
        #
        # StatementHandles that have already been finished will normally be
        # inoperable and unavailable for further use.
        #
        def finish
            @handle.finish
            @handle = dummy_handle
            clear_state!
        end

        #
        # Cancel the query, closing any open result cursors and truncating any result sets.
        #
        # The difference between this and #finish is that cancelled statements
        # may be re-executed.
        #
        def cancel
            @handle.raises_an_exception if finished?
            @handle.cancel if @fetchable
            @fetchable = false
            @row = nil
        end

        #
        # Obtains the column names for this query as an array.
        #
        def column_names
            @column_names ||= @handle.column_info.collect {|col| col['name'] }
        end

        #
        # Obtain the type mappings for the columns in this query based on
        # ColumnInfo data on the query.
        #
        # The result will be a position-dependent array of objects that conform
        # to the DBI::Type calling syntax.
        #
        def column_types
            @column_types ||= @handle.column_info.collect do |col| 
                                  if col['dbi_type']
                                      col['dbi_type']
                                  else
                                      DBI::TypeUtil.type_name_to_module(col['type_name'])
                                  end
            end
        end

        #
        # See BaseStatement#column_info.
        #
        def column_info
            @handle.column_info.collect {|col| ColumnInfo.new(col) }
        end

        #
        # Should return the row modified count as the result of statement execution.
        #
        # However, some low-level drivers do not supply this information or
        # supply misleading information (> 0 rows for read-only select
        # statements, f.e.)
        #
        def rows
            @handle.rows
        end

        #
        # See BaseStatement#fetch.
        #
        # fetch can also take a block which will be applied to each row in a
        # similar fashion to Enumerable#collect. See #each.
        #
        def fetch(&p)
            _fetch(:fetch_conversion_dbi_row, &p)
        end

        #
        # Synonym for #fetch with a block.
        #
        def each(&p)
            raise InterfaceError, "No block given" unless block_given?
            fetch(&p)
        end

        #
        # Similar to #fetch, but returns Array of Array instead of Array of
        # DBI::Row objects (and therefore does not perform type mapping). This
        # is basically a way to get the raw data from the DBD.
        #
        def fetch_array(&p)
            _fetch(:fetch_conversion_raw, &p)
        end

        #
        # Map the columns and results into an Array of Hash resultset.
        #
        # No type conversion is performed here. Expect this to change in 0.6.0.
        #
        def fetch_hash(&p)
            _fetch(:fetch_conversion_hash, &p)
        end

        #
        # Fetch `cnt` rows. Result is array of DBI::Row
        #
        def fetch_many(cnt)
            check_fetchable!

            rows = @handle.fetch_many(cnt) || []
            return rows.collect{|r| fetch_conversion_dbi_row(r)}
        end

        # 
        # Fetch the entire result set. Result is array of DBI::Row.
        #
        def fetch_all
            ret = []
            begin
                fetch do |r| ret << r end
            rescue Exception
            end
            ret
        end

        #
        # See BaseStatement#fetch_scroll.
        #
        def fetch_scroll(direction, offset=1)
            check_fetchable!

            row = @handle.fetch_scroll(direction, offset)
            if row.nil?
                #cancel
                return nil
            else
                @row.set_values(row)
                return @row
            end
        end

        # Get an attribute from the StatementHandle object.
        def [] (attr)
            @handle[attr]
        end

        # Set an attribute on the StatementHandle object.
        def []= (attr, val)
            @handle[attr] = val
        end
        
        protected

        # fetch() and friends
        #
        # _fetch method pulls results from the underlying driver, mapping
        # to user-requested formats via fetch_conversion_* methods
        #
        def _fetch(converter, &p)
            if !@row
                raise InterfaceError, "StatementHandle hasn't been executed yet"
            end
            check_fetchable!

            if p
                while res = @handle.fetch
                    yield __send__(converter, res)
                end
                nil
            end

            res = @handle.fetch
            return __send__(converter, res) if res
        end


        # Fetch conversion functions, mapping a @handle.result to the
        # caller's requested format (fetch_hash, etc.)
        def fetch_conversion_hash(raw_row)
            ::Hash[*column_names.zip(raw_row).flatten]
        end

        def fetch_conversion_raw(raw_row); raw_row end

        def fetch_conversion_dbi_row(raw_row)
            @row = @row.dup
            @row.set_values(raw_row)
            @row
        end

        # Boolean indicating whether this sth has been executed and
        # not cancelled nor finished, whether or not it is still fetchable
        def executed?
            return !@row.nil?
        end

        def check_fetchable!
            @handle.raises_an_exception if finished?

            if !@fetchable and @raise_error
                raise InterfaceError, "StatementHandle has no data for fetching"
            end
        end

        # clear the accessor vars associated with our handle
        def clear_state!
            @fetchable = false
            @column_names = nil
            @column_types = nil
            @row = nil
        end

    end # class StatementHandle
end
