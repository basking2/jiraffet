package com.github.basking2.jiraffet.db;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;

/**
 * A directory that is here and then gone. Used for testing things.
 */
public class EphemeralDirectory implements AutoCloseable {
    final Path temporaryDirectory;
    
    public EphemeralDirectory() throws IOException {
        this.temporaryDirectory = Files.createTempDirectory(getClass().getSimpleName());
    }
    
    public Path getTemporaryDirectory() {
        return temporaryDirectory;
    }

    @Override
    public void close() throws Exception {
        Files.walkFileTree(temporaryDirectory, new FileVisitor<Path>() {

            @Override
            public FileVisitResult postVisitDirectory(Path arg0, IOException arg1) throws IOException {
                Files.delete(arg0);

                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult preVisitDirectory(Path arg0, BasicFileAttributes arg1) throws IOException {
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(Path arg0, BasicFileAttributes arg1) throws IOException {
                Files.delete(arg0);

                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFileFailed(Path arg0, IOException arg1) throws IOException {
                return FileVisitResult.CONTINUE;
            }
            
        });
    }
    

}
