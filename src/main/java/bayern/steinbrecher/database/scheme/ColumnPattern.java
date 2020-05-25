package bayern.steinbrecher.database.scheme;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Objects;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;

/**
 * @param <T> The datatype of the values stored in the column.
 * @param <U> The type of objects the content of this column belongs to.
 * @author Stefan Huber
 * @since v0.1
 */
public abstract class ColumnPattern<T, U> {

    private static final Logger LOGGER = Logger.getLogger(ColumnPattern.class.getName());
    private final Pattern columnNamePattern;
    private final ColumnParser<T> parser;

    /**
     * @param columnNamePattern The RegEx column names have to match.
     * @param parser            The parser for converting values from and to their SQL representation.
     * @since v0.1
     */
    protected ColumnPattern(@NotNull String columnNamePattern, @NotNull ColumnParser<T> parser) {
        Objects.requireNonNull(columnNamePattern, "The pattern for the column name is required");
        Objects.requireNonNull(parser, "A parser which handles the column content is required");

        if (columnNamePattern.charAt(0) != '^' || !columnNamePattern.endsWith("$")) {
            LOGGER.log(Level.WARNING, "The pattern \"{0}\" is not encapsulated in \"^\" and \"$\".", columnNamePattern);
        }
        this.columnNamePattern = Pattern.compile(columnNamePattern, Pattern.CASE_INSENSITIVE | Pattern.UNICODE_CASE);
        this.parser = parser;
    }

    /*
     * @since v0.1
     */
    @NotNull
    public Pattern getColumnNamePattern() {
        return columnNamePattern;
    }

    /*
     * @since v0.1
     */
    public boolean matches(String columnName) {
        return getColumnNamePattern().matcher(columnName)
                .matches();
    }

    /*
     * @since v0.1
     */
    @NotNull
    public ColumnParser<T> getParser() {
        return parser;
    }

    /**
     * Parses the given value and sets it to the object of type {@link U}.
     *
     * @param toSet      The object to set the parsed value to.
     * @param columnName The column name matching this pattern to extract the key from.
     * @param value      The value to parse and to set.
     * @return The resulting object of type {@link U}.
     * @since v0.1
     */
    public final U combine(@NotNull U toSet, @NotNull String columnName, @Nullable String value) {
        if (getColumnNamePattern().matcher(columnName).matches()) {
            String valueToParse;
            if (value == null || value.equalsIgnoreCase("null")) {
                valueToParse = null;
            } else {
                valueToParse = value;
            }
            return combineImpl(toSet, columnName, valueToParse);
        } else {
            throw new IllegalArgumentException("The given column name does not match this pattern.");
        }
    }

    /*
     * @since v0.1
     */
    public abstract U combineImpl(@NotNull U toSet, @NotNull String columnName, @Nullable String valueToParse);

    /**
     * Checks whether this pattern reflects the same column names as the given object. NOTE It is only checked whether
     * their regex are identical not whether they express the same column names.
     *
     * @param obj The object to compare this column pattern with.
     * @return {@code true} only if this pattern reflects the same column names as the given object.
     * @since v0.1
     */
    @Override
    public boolean equals(Object obj) {
        boolean isEqual = false;
        if (obj instanceof ColumnPattern) {
            isEqual = ((ColumnPattern<?, ?>) obj).getColumnNamePattern()
                    .pattern()
                    .equals(columnNamePattern.pattern());
        }
        return isEqual;
    }

    /*
     * @since v0.1
     */
    @Override
    public int hashCode() {
        //CHECKSTYLE.OFF: MagicNumber - This is the default implementation of NetBeans.
        int hash = 5;
        hash = 97 * hash + Objects.hashCode(this.columnNamePattern);
        //CHECKSTYLE.ON: MagicNumber
        return hash;
    }

    /*
     * @since v0.1
     */
    @Override
    @NotNull
    public String toString() {
        return getColumnNamePattern()
                .pattern();
    }
}
