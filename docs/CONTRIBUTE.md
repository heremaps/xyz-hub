# Contribute

This document clarifies the needs when contributing to Naksha.

TBD

## Error handling

There are discussions ongoing about checked exceptions vs unchecked exceptions, for example Kotlin does not supported checked exceptions at all. Links:

- [Kotlin Exceptions](https://kotlinlang.org/docs/exceptions.html)
- [Java's checked exceptions were a mistake (Rod Waldhoff)](https://radio-weblogs.com/0122027/stories/2003/04/01/JavasCheckedExceptionsWereAMistake.html)
- [The Trouble with Checked Exceptions(Anders Hejlsberg)](https://www.artima.com/intv/handcuffs.html)
- [Exception Tunneling](http://wiki.c2.com/?ExceptionTunneling)

The basic problem that checked exceptions should solve is that we want to understand, when a function can produce an error and when not. We want the caller to either handle the error, or return the error to the caller, so the caller handles the error. In an ideal world we want to know either, which errors are possible, so we can decide, which ones we want to handle.

However, there are major gaps for this as soon as interfaces are used:

- If the interface does not declare an exception, and the implementation has to throw one, it will fall back to wrap the exception into a runtime exceptions, which restores the situation that we do not want to have.
- If the interface does declare that an exception can be thrown, and the implementation does never actually do, the caller needs a lot of try-catch blocks not worth anything, especially if handling the error at that place is not really possible.
- If we simply add a `throws Exception` we end up adding this to all methods, restoring the situation before checked exceptions.

So, ideally we do not want to be forced to handle exceptions, but we want to be informed about all possible errors. This allows us to decide which we handle, and which we ignore. For Naksha we solved this issue by only using unchecked exceptions and creating a feature request to IntelliJ to support [checked exceptions as unchecked exceptions](https://youtrack.jetbrains.com/issue/IDEA-325616/Support-Java-checked-exceptions-as-unchecked-exception-feature-improve-compatibility-with-Kotlin) using the [compiler plugin of Roger Keays](https://github.com/rogerkeays/unchecked).

However, as it is unlikely, that we get this done soon (or even if at all), we decided for now to introduce a solution on our self, that is not that perfect, but will do for us. We decided to simply wrap all checked exceptions in an own unchecked exception class named `UncheckedException`. When handling errors use the static method `cause(Throwable)` of this class to unpack all well-known exceptions. We will document all possible errors simply in the JavaDoc using `@throws`. When checked exceptions are documented, they will only be available after unpacking via `cause(Throwable)`.

The method `unchecked(Throwable)` will return the given exception as unchecked exception. The method returns either the given exception, when it is an unchecked exception, so when being a **RuntimeException**, otherwise it will create a new **UncheckedException** or **UncheckedIOException** wrapper to make it an unchecked exception and return this. The next call to `cause(Throwable)` will unpack the checked exception.

This is not that elegant, because interfaces sitting between the implementation, and the caller, will hide errors, but maybe JetBrains will come to our rescue later. However, it seems to be the best thing we can do for now and this creates a compatibility with Kotlin.

This little example class shows error reporting and error handling:

```java
import static com.here.naksha.lib.core.exceptions.CheckedException.unchecked;
import static com.here.naksha.lib.core.exceptions.CheckedException.cause;
import java.sql.SQLException;

public class Demo extends Something {

  /**
   * Return some string.
   * @return Some string.
   * @throws SQLException If an error occurred while reading from the database.
   * @throws java.io.IOException If reading from disk failed.
   */
  public @NotNull String foo() {
    try (final Statement stmt = executeSql()) {
      // Do something ...
      return "Hello World!";
    } catch (final Exception e) {
      // We do not want to handle any error.
      throw unchecked(e);
    }
  }

  /**
   * Print the result of foo.
   * @throws IllegalStateException If the method foo returned null.
   */
  public void bar() {
    try {
      final String result = foo();
      if (result == null) throw new IllegalStateException("Result of foo must not be null");
      System.out.println(result);
    } catch (final Throwable o) {
      final Throwable t = cause(o);
      if (t instanceof SQLException e) {
        System.out.println(e.getSQLState());
      } if (t instanceof IOException e) {
        System.out.println(e.getMessage());
      } else {
        // We can't handle this, forward to parent.
        throw unchecked(o);
      }
    }
  }
}
```

Another thing this is example shows is that our request to JetBrains is not trivial, because a caller of the `foo()` method potentially might not expose by itself that it throws an `SQLException` or an `IOExeception`, therefore the IDE need to recursively trace the call to detect all the possible exceptions. No method ever need to be reviewed twice, which at least avoids endless loops or endless recursions, but still it can be time-consuming to collect all exceptions possible. Additionally, all consumed exceptions must be removed, when a method in the call-stack handles it. However, I believe it is doable and hope someone at JetBrains will do it. If not, we still have the option to develop our own plugin for IntelliJ.
