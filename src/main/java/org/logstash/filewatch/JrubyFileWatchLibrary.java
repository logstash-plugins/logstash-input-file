package org.logstash.filewatch;

/**
 * Created with IntelliJ IDEA. User: efrey Date: 6/11/13 Time: 11:00 AM To
 * change this template use File | Settings | File Templates.
 *
 * http://bugs.sun.com/view_bug.do?bug_id=6357433
 * [Guy] modified original to be a proper JRuby class
 * [Guy] do we need this anymore? JRuby 1.7+ uses new Java 7 File API
 *
 *
 * fnv code extracted and modified from https://github.com/jakedouglas/fnv-java
 */

import jnr.ffi.Runtime;
import jnr.posix.HANDLE;
import jnr.posix.JavaLibCHelper;
import jnr.posix.POSIX;
import jnr.posix.WindowsLibC;
import jnr.posix.WindowsPOSIX;
import jnr.posix.util.WindowsHelpers;
import jnr.posix.windows.WindowsFileInformationByHandle;
import org.jruby.Ruby;
import org.jruby.RubyBignum;
import org.jruby.RubyClass;
import org.jruby.RubyFixnum;
import org.jruby.RubyIO;
import org.jruby.RubyModule;
import org.jruby.RubyNumeric;
import org.jruby.RubyObject;
import org.jruby.RubyString;
import org.jruby.anno.JRubyClass;
import org.jruby.anno.JRubyMethod;
import org.jruby.ext.ffi.Factory;
import org.jruby.ext.ffi.MemoryIO;
import org.jruby.ext.ffi.Pointer;
import org.jruby.runtime.Arity;
import org.jruby.runtime.Block;
import org.jruby.runtime.ThreadContext;
import org.jruby.runtime.builtin.IRubyObject;
import org.jruby.runtime.load.Library;
import org.jruby.util.io.OpenFile;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.channels.Channel;
import java.nio.channels.FileChannel;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

@SuppressWarnings("ClassUnconnectedToPackage")
public class JrubyFileWatchLibrary implements Library {

    private static final BigInteger INIT32 = new BigInteger("811c9dc5", 16);
    private static final BigInteger INIT64 = new BigInteger("cbf29ce484222325", 16);
    private static final BigInteger PRIME32 = new BigInteger("01000193", 16);
    private static final BigInteger PRIME64 = new BigInteger("100000001b3", 16);
    private static final BigInteger MOD32 = new BigInteger("2").pow(32);
    private static final BigInteger MOD64 = new BigInteger("2").pow(64);

    // private static final int GENERIC_ALL = 268435456;
    private static final int GENERIC_READ = -2147483648;
    // private static final int GENERIC_WRITE = 1073741824;
    // private static final int GENERIC_EXECUTE = 33554432;
    // private static final int FILE_SHARE_DELETE = 4;
    private static final int FILE_SHARE_READ = 1;
    private static final int FILE_SHARE_WRITE = 2;
    // private static final int CREATE_ALWAYS = 2;
    // private static final int CREATE_NEW = 1;
    // private static final int OPEN_ALWAYS = 4;
    private static final int OPEN_EXISTING = 3;
    // private static final int TRUNCATE_EXISTING = 5;
    private static final int FILE_FLAG_BACKUP_SEMANTICS = 33554432;
    // private static final int FILE_ATTRIBUTE_READONLY = 1;

    @Override
    public final void load(final Ruby runtime, final boolean wrap) {
        final RubyModule module = runtime.defineModule("FileWatch");

        RubyClass clazz = runtime.defineClassUnder("FileExt", runtime.getObject(), JrubyFileWatchLibrary.RubyFileExt::new, module);
        clazz.defineAnnotatedMethods(JrubyFileWatchLibrary.RubyFileExt.class);

        clazz = runtime.defineClassUnder("Fnv", runtime.getObject(), JrubyFileWatchLibrary.Fnv::new, module);
        clazz.defineAnnotatedMethods(JrubyFileWatchLibrary.Fnv.class);

    }

    @JRubyClass(name = "FileExt")
    public static class RubyFileExt extends RubyObject {

        public RubyFileExt(final Ruby runtime, final RubyClass meta) {
            super(runtime, meta);
        }

        public RubyFileExt(final RubyClass meta) {
            super(meta);
        }

        @JRubyMethod(name = "open", required = 1, meta = true)
        public static IRubyObject open(final ThreadContext context, final IRubyObject self, final RubyString path) throws IOException {
            final Path javapath = FileSystems.getDefault().getPath(path.asJavaString());
            final Channel channel = FileChannel.open(javapath, StandardOpenOption.READ);
            final RubyIO irubyobject = new RubyWinIO(context.runtime, channel);
            return irubyobject;
        }

        @JRubyMethod(name = "io_handle", required = 1, meta = true)
        public static IRubyObject ioHandle(final ThreadContext context, final IRubyObject self, final IRubyObject object, Block block) {
            final Ruby runtime = context.runtime;
            if (!block.isGiven()) {
                throw runtime.newArgumentError(0, 1);
            }
            if (object instanceof RubyWinIO) {
                final RubyWinIO rubyWinIO = (RubyWinIO) object;
                final OpenFile fptr = rubyWinIO.getOpenFileChecked();
                final boolean locked = fptr.lock();
                try {
                    fptr.checkClosed();
                    if (rubyWinIO.isDirect()) {
                        final MemoryIO memoryio = Factory.getInstance().wrapDirectMemory(runtime, rubyWinIO.getAddress());
                        final Pointer pointer = new Pointer(runtime, memoryio);
                        return block.yield(context, pointer);
                    }
                } finally {
                    if (locked) {
                        fptr.unlock();
                    }
                }
            } else {
                System.out.println("Required argument is not a WinIO instance");
            }
            return runtime.newString();
        }

        //@JRubyMethod(name = "io_inode", required = 1, meta = true)
        public static RubyString ioInode(final ThreadContext context, final IRubyObject self, final IRubyObject object) {
            final Ruby runtime = context.runtime;
            if (!(object instanceof RubyIO)) {
                System.out.println("Required argument is not an IO instance");
                return runtime.newString();
            }
            final RubyIO rubyIO = (RubyIO) object;
            final OpenFile fptr = rubyIO.getOpenFileChecked();
            final boolean locked = fptr.lock();
            String inode = "";
            try {
                fptr.checkClosed();
                final POSIX posix = runtime.getPosix();
                final int realFileno = fptr.fd().realFileno;
                if (posix.isNative() && posix instanceof WindowsPOSIX && realFileno != -1) {
                    final WindowsPOSIX winposix = (WindowsPOSIX) posix;
                    final WindowsLibC wlibc = (WindowsLibC) winposix.libc();
                    final WindowsFileInformationByHandle info = new WindowsFileInformationByHandle(Runtime.getRuntime(runtime.getPosix().libc()));
                    final HANDLE handle = JavaLibCHelper.gethandle(JavaLibCHelper.getDescriptorFromChannel(fptr.fd().chFile));
                    if (handle.isValid()) {
                        if (wlibc.GetFileInformationByHandle(handle, info) > 0) {
                            inode = info.getIdentifier();
                        } else {
                            System.out.println("Could not 'GetFileInformationByHandle' from handle");
                        }
                    } else {
                        System.out.println("Could not derive 'HANDLE' from Ruby IO instance via io.getOpenFileChecked().fd().chFile");
                    }
                }
            } finally {
                if (locked) {
                    fptr.unlock();
                }
            }
            return runtime.newString(inode);
        }

        //@JRubyMethod(name = "path_inode", required = 1, meta = true)
        public static RubyString pathInode(final ThreadContext context, final IRubyObject self, final RubyString path) {
            final Ruby runtime = context.runtime;
            final POSIX posix = runtime.getPosix();
            String inode = "";
            if (posix.isNative() && posix.libc() instanceof WindowsLibC) {
                final WindowsLibC wlibc = (WindowsLibC) posix.libc();
                final byte[] wpath = WindowsHelpers.toWPath(path.toString());
                final HANDLE handle = wlibc.CreateFileW(wpath, GENERIC_READ, FILE_SHARE_READ | FILE_SHARE_WRITE, null, OPEN_EXISTING, FILE_FLAG_BACKUP_SEMANTICS, 0);
                if (handle.isValid()) {
                    final WindowsFileInformationByHandle info = new WindowsFileInformationByHandle(Runtime.getRuntime(runtime.getPosix().libc()));
                    if (wlibc.GetFileInformationByHandle(handle, info) > 0) {
                        inode = info.getIdentifier();
                    } else {
                        System.out.println("Could not 'GetFileInformationByHandle' from handle");
                    }
                    wlibc.CloseHandle(handle);
                } else {
                    System.out.printf("Could not open file via 'CreateFileW' on path: %s", path.toString());
                }
            }
            return runtime.newString(inode);
        }
    }

    // This class may be used by fingerprinting in the future
    @SuppressWarnings({"NewMethodNamingConvention", "ChainOfInstanceofChecks"})
    @JRubyClass(name = "Fnv")
    public static class Fnv extends RubyObject {

        private byte[] bytes;
        private long size;
        private boolean open;

        public Fnv(final Ruby runtime, final RubyClass metaClass) {
            super(runtime, metaClass);
        }

        public Fnv(final RubyClass metaClass) {
            super(metaClass);
        }

        @JRubyMethod(name = "coerce_bignum", meta = true, required = 1)
        public static IRubyObject coerceBignum(final ThreadContext ctx, final IRubyObject recv, final IRubyObject rubyObject) {
            if (rubyObject instanceof RubyBignum) {
                return rubyObject;
            }
            if (rubyObject instanceof RubyFixnum) {
                return RubyBignum.newBignum(ctx.runtime, ((RubyNumeric) rubyObject).getBigIntegerValue());
            }
            throw ctx.runtime.newRaiseException(ctx.runtime.getClass("StandardError"), "Can't coerce");
        }

        // def initialize(data)
        @JRubyMethod(name = "initialize", required = 1)
        public IRubyObject rubyInitialize(final ThreadContext ctx, final RubyString data) {
            bytes = data.getBytes();
            size = (long) bytes.length;
            open = true;
            return ctx.nil;
        }

        @JRubyMethod(name = "close")
        public IRubyObject close(final ThreadContext ctx) {
            open = false;
            bytes = null;
            return ctx.nil;
        }

        @JRubyMethod(name = "open?")
        public IRubyObject open_p(final ThreadContext ctx) {
            if(open) {
                return ctx.runtime.getTrue();
            }
            return ctx.runtime.getFalse();
        }

        @JRubyMethod(name = "closed?")
        public IRubyObject closed_p(final ThreadContext ctx) {
            if(open) {
                return ctx.runtime.getFalse();
            }
            return ctx.runtime.getTrue();
        }

        @JRubyMethod(name = "fnv1a32", optional = 1)
        public IRubyObject fnv1a_32(final ThreadContext ctx, final IRubyObject[] args) {
            IRubyObject[] args1 = args;
            if(open) {
                args1 = Arity.scanArgs(ctx.runtime, args1, 0, 1);
                return RubyBignum.newBignum(ctx.runtime, common_fnv(args1[0], INIT32, PRIME32, MOD32));
            }
            throw ctx.runtime.newRaiseException(ctx.runtime.getClass("StandardError"), "Fnv instance is closed!");
        }

        @JRubyMethod(name = "fnv1a64", optional = 1)
        public IRubyObject fnv1a_64(final ThreadContext ctx, final IRubyObject[] args) {
            IRubyObject[] args1 = args;
            if(open) {
                args1 = Arity.scanArgs(ctx.runtime, args1, 0, 1);
                return RubyBignum.newBignum(ctx.runtime, common_fnv(args1[0], INIT64, PRIME64, MOD64));
            }
            throw ctx.runtime.newRaiseException(ctx.runtime.getClass("StandardError"), "Fnv instance is closed!");
        }

        private long convertLong(final IRubyObject obj) {
            if(obj instanceof RubyNumeric) {
                return ((RubyNumeric) obj).getLongValue();
            }
            return size;
        }

        private BigInteger common_fnv(final IRubyObject len, final BigInteger hash, final BigInteger prime, final BigInteger mod) {
            long converted = convertLong(len);

            if (converted > size) {
                converted = size;
            }

            BigInteger tempHash = hash;
            for (int idx = 0; (long) idx < converted; idx++) {
                tempHash = tempHash.xor(BigInteger.valueOf((long) ((int) bytes[idx] & 0xff)));
                tempHash = tempHash.multiply(prime).mod(mod);
            }

            return tempHash;
        }
    }

}
