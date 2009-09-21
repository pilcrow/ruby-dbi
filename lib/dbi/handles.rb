#
# Dispatch classes (Handle, DriverHandle, DatabaseHandle and StatementHandle)
#

module DBI
    #
    # Base class for all handles.
    #
    class Handle
        attr_reader :trace_mode, :trace_output
        attr_reader :handle 
        attr :convert_types, true

        def initialize(handle, convert_types=true)
            @handle = handle
            @trace_mode = @trace_output = nil
            @convert_types = convert_types
        end

        # Please seee DBI.trace.
        def trace(mode=nil, output=nil)
            # FIXME trace
            raise InterfaceError, "the trace module has been removed until it actually works."
            @trace_mode   = mode   || @trace_mode   || DBI::DEFAULT_TRACE_MODE
            @trace_output = output || @trace_output || DBI::DEFAULT_TRACE_OUTPUT
        end

        #
        # Leverage a driver-specific method. The method name will have "__"
        # prepended to them before calling, and the DBD must define them as
        # such for them to work.
        #
        def func(function, *values, &block)
            @handle.send('__' + function.to_s, *values, &block)
        rescue
            # This is a bit of work to distinguish caller error (your fault!)
            # from implementor error (my fault!)
            name = '__' + function.to_s
            caller_err = nil

            case $!
            when NoMethodError
                unless @handle.respond_to?(name)
                    caller_err = "not available"
                end
            when ArgumentError
                m = @handle.method(name)
                unless DBI::Utils::arity_satisfied?(m, values)
                    caller_err = "wrong number of arguments (#{values.size} for #{DBI::Utils::hard_arity(m)})"
                end
            when LocalJumpError
                # good effort, works under MRI 1.8 and 1.9.
                # An alternative is to measure relative stack depth
                # difference between caller(0) and $!.backtrace (which
                # difference varies from 1.8 to 1.9)
                if !block_given? and $!.backtrace[0] =~ %r"\b#{Regexp.quote(name)}\b"
                    caller_err = "no block given"
                end
            end

            raise InterfaceError, "<#{function}> #{caller_err}" if caller_err
            raise # it's the implementor's fault, pass it on
        end
    end #-- class Handle
end #-- module DBI

require 'dbi/handles/driver'
require 'dbi/handles/database'
require 'dbi/handles/statement'
