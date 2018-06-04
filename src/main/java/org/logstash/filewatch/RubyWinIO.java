package org.logstash.filewatch;

import jnr.posix.HANDLE;
import jnr.posix.JavaLibCHelper;
import org.jruby.Ruby;
import org.jruby.RubyBoolean;
import org.jruby.RubyIO;
import org.jruby.anno.JRubyClass;
import org.jruby.anno.JRubyMethod;
import org.jruby.runtime.ThreadContext;
import org.jruby.runtime.builtin.IRubyObject;
import org.jruby.util.io.OpenFile;

import java.nio.channels.Channel;

@JRubyClass(name = "WinIO")
public class RubyWinIO extends RubyIO {
    private boolean valid;
    private boolean direct;
    private long address;

    public RubyWinIO(Ruby runtime, Channel channel) {
        super(runtime, channel);
        final OpenFile fptr = getOpenFileChecked();
        final boolean locked = fptr.lock();
        try {
            fptr.checkClosed();
            final HANDLE handle = JavaLibCHelper.gethandle(JavaLibCHelper.getDescriptorFromChannel(fptr.fd().chFile));
            if (handle.isValid()) {
                direct = handle.toPointer().isDirect();
                address = handle.toPointer().address();
                valid = true;
            } else {
                direct = false;
                address = 0L;
                valid = false;
            }
        } finally {
            if (locked) {
                fptr.unlock();
            }
        }
    }

    @JRubyMethod(name = "valid?")
    public RubyBoolean valid_p(ThreadContext context) {
        return context.runtime.newBoolean(valid);
    }

    @Override
    @JRubyMethod
    public IRubyObject close() {
        direct = false;
        address = 0L;
        return super.close();
    }

    final public boolean isDirect() {
        return direct;
    }

    final public long getAddress() {
        return address;
    }
}
