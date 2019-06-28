package io.burt.athena.support;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public interface PomVersionLoader {
    default Optional<String> pomVersion() throws IOException {
        Pattern versionPattern = Pattern.compile("<version>([^<]+)</version>");
        Path pomPath = Paths.get("pom.xml").toAbsolutePath();
        try (Stream<String> stream = Files.lines(pomPath)) {
            return stream.filter(versionPattern.asPredicate()).findFirst().flatMap(versionLine -> {
                Matcher matcher = versionPattern.matcher(versionLine);
                matcher.find();
                return Optional.of(matcher.group(1));
            });
        }
    }

    default Optional<int[]> pomVersionComponents() throws IOException {
        Pattern versionComponentsPattern = Pattern.compile("(\\d+)\\.(\\d+)\\.(\\d+)(?:-\\w+)?");
        return pomVersion().flatMap(versionString -> {
            Matcher matcher = versionComponentsPattern.matcher(versionString);
            if (matcher.matches()) {
                return Optional.of(new int[]{Integer.valueOf(matcher.group(1)), Integer.valueOf(matcher.group(2))});
            } else {
                return Optional.empty();
            }
        });
    }
}
