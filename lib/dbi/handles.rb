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
            caller_error = \
                case $!
                when NoMethodError
                    "not available" unless @handle.respond_to?(name)
                when ArgumentError
                    method = @handle.method(name)
                    "wrong number of arguments (#{values.size} for #{_hard_arity(method)}" unless _args_match_arity?(method, values)
                when LocalJumpError
                    # good effort, works under MRI 1.8 and 1.9
                    "no block given" if $!.backtrace[0] =~ %r"\b#{Regexp.quote(name)}\b"
                end
            raise InterfaceError, "<#{function}> #{caller_error}" if caller_error
            raise # it's the implementor's fault
        end

        private
        # return the minimum no. of necessary arguments for method
        def _args_match_arity?(method, args)
            arity = method.arity
            return (arity >= 0)                ?
                   args.size == arity          :
                   args.size >= (arity.abs - 1)
        end

        # return the minimum no. of necessary arguments for method
        def _hard_arity(method)
            arity = method.arity
            return (arity >= 0) ? arity : (arity.abs - 1)
        end

    end
end

require 'dbi/handles/driver'
require 'dbi/handles/database'
require 'dbi/handles/statement'
