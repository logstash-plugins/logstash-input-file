import org.jruby.Ruby;
import org.jruby.runtime.load.BasicLibraryService;
import org.logstash.filewatch.JrubyFileWatchLibrary;

public class JrubyFileWatchService implements BasicLibraryService {
    @Override
    public final boolean basicLoad(final Ruby runtime) {
        new JrubyFileWatchLibrary().load(runtime, false);
        return true;
    }
}
