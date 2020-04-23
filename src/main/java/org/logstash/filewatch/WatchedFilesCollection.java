package org.logstash.filewatch;

import org.jruby.Ruby;
import org.jruby.RubyClass;
import org.jruby.RubyFloat;
import org.jruby.RubyHash;
import org.jruby.RubyObject;
import org.jruby.RubyString;
import org.jruby.anno.JRubyMethod;
import org.jruby.javasupport.JavaUtil;
import org.jruby.runtime.Block;
import org.jruby.runtime.ThreadContext;
import org.jruby.runtime.Visibility;
import org.jruby.runtime.builtin.IRubyObject;
import org.jruby.runtime.callsite.CachingCallSite;
import org.jruby.runtime.callsite.FunctionalCachingCallSite;

import java.util.Comparator;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * FileWatch::WatchedFilesCollection (native) part.
 *
 * Implemented here to avoid Ruby->Java type casting (which JRuby provides no control of as of 9.2)
 * We could have used Ruby's SortedSet but it does not provide support for custom comparators.
 */
public class WatchedFilesCollection extends RubyObject {

    private SortedMap<IRubyObject, RubyString> files; // FileWatch::WatchedFile -> String
    private RubyHash filesInverse;
    private transient CachingCallSite watchedFileSite;

    public WatchedFilesCollection(Ruby runtime, RubyClass metaClass) {
        super(runtime, metaClass);
    }

    static void load(Ruby runtime) {
        runtime.getOrCreateModule("FileWatch")
               .defineClassUnder("WatchedFilesCollection", runtime.getObject(), WatchedFilesCollection::new)
               .defineAnnotatedMethods(WatchedFilesCollection.class);
    }

    @JRubyMethod
    public IRubyObject initialize(final ThreadContext context, IRubyObject settings) {
        final String sort_by = settings.callMethod(context, "file_sort_by").asJavaString();
        final String sort_direction = settings.callMethod(context, "file_sort_direction").asJavaString();

        final String method;
        Comparator<IRubyObject> comparator;
        switch (sort_by) {
            case "last_modified" :
                method = "modified_at";
                comparator = (file1, file2) -> {
                    RubyFloat mtime1 = watchedFileCallMethod(context, file1).convertToFloat();
                    RubyFloat mtime2 = watchedFileCallMethod(context, file2).convertToFloat();
                    return Double.compare(mtime1.getDoubleValue(), mtime2.getDoubleValue());
                };
                break;
            case "path" :
                method = "path";
                comparator = (file1, file2) -> {
                    RubyString path1 = watchedFileCallMethod(context, file1).convertToString();
                    RubyString path2 = watchedFileCallMethod(context, file2).convertToString();
                    return path1.op_cmp(path2);
                };
                break;
            default :
                throw context.runtime.newArgumentError("sort_by: '" + sort_by + "' not supported");
        }

        if ("desc".equals(sort_direction)) {
            comparator = comparator.reversed();
        }

        this.watchedFileSite = new FunctionalCachingCallSite(method);

        this.files = new TreeMap<>(comparator);
        this.filesInverse = RubyHash.newHash(context.runtime);

        variableTableStore("@files", JavaUtil.convertJavaToRuby(context.runtime, this.files));
        variableTableStore("@files_inverse", this.filesInverse);

        return this;
    }

    @JRubyMethod // synchronize { @files.values.to_a }
    public synchronized IRubyObject paths(ThreadContext context) {
        IRubyObject[] values = this.files.values().stream().toArray(IRubyObject[]::new);
        return context.runtime.newArrayNoCopy(values);
    }

    // NOTE: needs to return properly ordered files (can not use @files_inverse)
    @JRubyMethod // synchronize { @files.key_set.to_a }
    public synchronized IRubyObject files(ThreadContext context) {
        IRubyObject[] keys = this.files.keySet().stream().toArray(IRubyObject[]::new);
        return context.runtime.newArrayNoCopy(keys);
    }

    // internal helpers for the rest of watched_file_collection.rb :

    @JRubyMethod(visibility = Visibility.PRIVATE)
    public IRubyObject _put_file(ThreadContext context, IRubyObject file, IRubyObject path) {
        RubyString prev_path = this.files.put(file, (RubyString) path);
        return prev_path == null ? context.nil : prev_path;
    }

    @JRubyMethod(visibility = Visibility.PRIVATE)
    public IRubyObject _remove_file(ThreadContext context, IRubyObject file) {
        IRubyObject removed = this.files.remove(file);
        return removed == null ? context.nil : removed;
    }

    @JRubyMethod(visibility = Visibility.PRIVATE)
    public IRubyObject synchronize(ThreadContext context, Block block) {
        synchronized (this) { return block.yield(context, this); }
    }

    private IRubyObject watchedFileCallMethod(ThreadContext context, IRubyObject watched_file) {
        return watchedFileSite.call(context, watched_file, watched_file);
    }
}
