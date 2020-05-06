package org.logstash.filewatch;

import org.jruby.Ruby;
import org.jruby.RubyArray;
import org.jruby.RubyBoolean;
import org.jruby.RubyClass;
import org.jruby.RubyFloat;
import org.jruby.RubyHash;
import org.jruby.RubyObject;
import org.jruby.RubyString;
import org.jruby.anno.JRubyMethod;
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
 * FileWatch::WatchedFilesCollection for managing paths mapped to (watched) files.
 *
 * Implemented in native to avoid Ruby->Java type casting (which JRuby provides no control of as of 9.2).
 * The collection already has a noticeable footprint when 10_000s of files are being watched at once, having
 * the implementation in Java reduces 1000s of String conversions on every watch re-stat tick.
 */
public class WatchedFilesCollection extends RubyObject {

    // we could have used Ruby's SortedSet but it does not provide support for custom comparators
    private SortedMap<IRubyObject, RubyString> files; // FileWatch::WatchedFile -> String
    private RubyHash filesInverse; // String -> FileWatch::WatchedFile
    private String sortBy;

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

        Comparator<IRubyObject> comparator;
        switch (sort_by) {
            case "last_modified" :
                sortBy = "modified_at";
                comparator = (file1, file2) -> {
                    if (file1 == file2) return 0; // fast shortcut
                    RubyFloat mtime1 = modified_at(context, file1);
                    RubyFloat mtime2 = modified_at(context, file2);
                    int cmp = Double.compare(mtime1.getDoubleValue(), mtime2.getDoubleValue());
                    // if mtime same (rare unless file1 == file2) - order consistently
                    if (cmp == 0) return path(context, file1).op_cmp(path(context, file2));
                    return cmp;
                };
                break;
            case "path" :
                sortBy = "path";
                comparator = (file1, file2) -> path(context, file1).op_cmp(path(context, file2));
                break;
            default :
                throw context.runtime.newArgumentError("sort_by: '" + sort_by + "' not supported");
        }
        switch (sort_direction) {
            case "asc" :
                // all good - comparator uses ascending order
                break;
            case "desc" :
                comparator = comparator.reversed();
                break;
            default :
                throw context.runtime.newArgumentError("sort_direction: '" + sort_direction + "' not supported");
        }

        this.files = new TreeMap<>(comparator);
        this.filesInverse = RubyHash.newHash(context.runtime);

        // variableTableStore("@files", JavaUtil.convertJavaToRuby(context.runtime, this.files));
        // variableTableStore("@files_inverse", this.filesInverse);

        return this;
    }

    @JRubyMethod
    public IRubyObject add(ThreadContext context, IRubyObject file) {
        RubyString path = getFilePath(context, file);
        synchronized (this) {
            RubyString prev_path = this.files.put(file, path);
            assert prev_path == null || path.equals(prev_path); // file's path should not change!
            this.filesInverse.op_aset(context, path, file);
        }
        return path;
    }

    private static RubyString getFilePath(ThreadContext context, IRubyObject file) {
        IRubyObject path = file.callMethod(context, "path");
        if (!(path instanceof RubyString)) {
            throw context.runtime.newTypeError("expected file.path to return String but did not file: " + file.inspect());
        }
        if (!path.isFrozen()) path = ((RubyString) path).dupFrozen(); // path = path.dup.freeze
        return (RubyString) path;
    }

    @JRubyMethod
    public IRubyObject remove_paths(ThreadContext context, IRubyObject arg) {
        IRubyObject[] paths;
        if (arg instanceof RubyArray) {
            paths = ((RubyArray) arg).toJavaArray();
        } else {
            paths = new IRubyObject[] { arg };
        }

        int removedCount = 0;
        synchronized (this) {
            for (final IRubyObject path : paths) {
                if (removePath(context, path.convertToString())) removedCount++;
            }
        }
        return context.runtime.newFixnum(removedCount);
    }

    private boolean removePath(ThreadContext context, RubyString path) {
        IRubyObject file = this.filesInverse.delete(context, path, Block.NULL_BLOCK);
        if (file.isNil()) return false;
        return this.files.remove(file) != null;
    }

    @JRubyMethod // synchronize { @files_inverse[path] }
    public synchronized IRubyObject get(ThreadContext context, IRubyObject path) {
        return this.filesInverse.op_aref(context, path);
    }

    @JRubyMethod // synchronize { @files.size }
    public synchronized IRubyObject size(ThreadContext context) {
        return context.runtime.newFixnum(this.files.size());
    }

    @JRubyMethod(name = "empty?") // synchronize { @files.empty? }
    public synchronized IRubyObject empty_p(ThreadContext context) {
        return context.runtime.newBoolean(this.files.isEmpty());
    }

    @JRubyMethod
    public synchronized IRubyObject each_file(ThreadContext context, Block block) {
        for (IRubyObject watched_file : this.files.keySet()) {
            block.yield(context, watched_file);
        }
        return context.nil;
    }

    @JRubyMethod // synchronize { @files.values.to_a }
    public IRubyObject paths(ThreadContext context) {
        IRubyObject[] values;
        synchronized (this) {
            values = this.files.values().stream().toArray(IRubyObject[]::new);
        }
        return context.runtime.newArrayNoCopy(values);
    }

    // NOTE: needs to return properly ordered files (can not use @files_inverse)
    @JRubyMethod // synchronize { @files.key_set.to_a }
    public IRubyObject files(ThreadContext context) {
        IRubyObject[] keys;
        synchronized (this) {
            keys = this.files.keySet().stream().toArray(IRubyObject[]::new);
        }
        return context.runtime.newArrayNoCopy(keys);
    }


    @JRubyMethod
    public IRubyObject update(ThreadContext context, IRubyObject file) {
        // NOTE: modified_at might change on restat - to cope with that we need to potentially
        // update the sorted collection, on such changes (when file_sort_by: last_modified) :
        if (!"modified_at".equals(sortBy)) return context.nil;

        RubyString path = getFilePath(context, file);
        synchronized (this) {
            this.files.remove(file); // we need to "re-sort" changed file -> remove and add it back
            modified_at(context, file, context.tru); // file.modified_at(update: true)
            RubyString prev_path = this.files.put(file, path);
            assert prev_path == null;
        }
        return context.tru;
    }

    @JRubyMethod(required = 1, visibility = Visibility.PRIVATE)
    @Override
    public IRubyObject initialize_copy(IRubyObject original) {
        final Ruby runtime = getRuntime();
        if (!(original instanceof WatchedFilesCollection)) {
            throw runtime.newTypeError("Expecting an instance of class WatchedFilesCollection");
        }

        WatchedFilesCollection proto = (WatchedFilesCollection) original;

        this.files = new TreeMap<>(proto.files.comparator());
        synchronized (proto) {
            this.files.putAll(proto.files);
            this.filesInverse = (RubyHash) proto.filesInverse.dup(runtime.getCurrentContext());
        }

        return this;
    }

    @Override
    public IRubyObject inspect() {
        return getRuntime().newString("#<" + metaClass.getRealClass().getName() + ": size=" + this.files.size() + ">");
    }

    private static final CachingCallSite modified_at_site = new FunctionalCachingCallSite("modified_at");
    private static final CachingCallSite path_site = new FunctionalCachingCallSite("path");

    private static RubyString path(ThreadContext context, IRubyObject watched_file) {
        return path_site.call(context, watched_file, watched_file).convertToString();
    }

    private static RubyFloat modified_at(ThreadContext context, IRubyObject watched_file) {
        return modified_at_site.call(context, watched_file, watched_file).convertToFloat();
    }

    private static RubyFloat modified_at(ThreadContext context, IRubyObject watched_file, RubyBoolean update) {
        return modified_at_site.call(context, watched_file, watched_file, update).convertToFloat();
    }

}
