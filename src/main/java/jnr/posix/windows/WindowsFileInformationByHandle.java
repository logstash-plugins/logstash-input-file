package jnr.posix.windows;
/*
This, sadly can't be used.
See JrubyFileWatchLibrary class
The jnr jar is loaded by a different class loader than our jar (in rspec anyway)
Even though the package is the same, Java restricts access to `dwVolumeSerialNumber` in the super class
We have to continue to use FFI in Ruby.
*/

public class WindowsFileInformationByHandle extends WindowsByHandleFileInformation {
    public WindowsFileInformationByHandle(jnr.ffi.Runtime runtime) {
        super(runtime);
    }

    public java.lang.String getIdentifier() {
        StringBuilder builder = new StringBuilder();
        builder.append(dwVolumeSerialNumber.intValue());
        builder.append("-");
        builder.append(nFileIndexHigh.intValue());
        builder.append("-");
        builder.append(nFileIndexLow.intValue());
        return builder.toString();
    }
}
