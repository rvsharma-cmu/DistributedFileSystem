package common;

import java.io.*;
import java.util.*;

/**
 * Distributed filesystem paths.
 * 
 * <p>
 * Objects of type <code>Path</code> are used by all filesystem interfaces. Path
 * objects are immutable.
 * 
 * <p>
 * The string representation of paths is a forward-slash-delimeted sequence of
 * path components. The root directory is represented as a single forward slash.
 * 
 * <p>
 * The colon (<code>:</code>) and forward slash (<code>/</code>) characters are
 * not permitted within path components. The forward slash is the delimeter, and
 * the colon is reserved as a delimeter for application use.
 */
public class Path implements Iterable<String>, Comparable<Path>, Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	/** Creates a new path which represents the root directory. */

	private List<String> pathList;

	public Path() {
		pathList = new ArrayList<String>();
		// throw new UnsupportedOperationException("not implemented");
	}

	/**
	 * Creates a new path by appending the given component to an existing path.
	 * 
	 * @param path      The existing path.
	 * @param component The new component.
	 * @throws IllegalArgumentException If <code>component</code> includes the
	 *                                  separator, a colon, or
	 *                                  <code>component</code> is the empty string.
	 */
	public Path(Path path, String component) {
		// throw new UnsupportedOperationException("not implemented");

		if (component.isEmpty() || component.contains(":") || component.contains("/")) {
			throw new IllegalArgumentException("component for path cotains illegal  argument");
		}

		this.pathList = new ArrayList<String>();
		this.pathList.addAll(path.pathList);
		pathList.add(component);
	}

	/**
	 * Creates a new path from a path string.
	 * 
	 * <p>
	 * The string is a sequence of components delimited with forward slashes. Empty
	 * components are dropped. The string must begin with a forward slash.
	 * 
	 * @param path The path string.
	 * @throws IllegalArgumentException If the path string does not begin with a
	 *                                  forward slash, or if the path contains a
	 *                                  colon character.
	 */
	public Path(String path) {
		// throw new UnsupportedOperationException("not implemented");

		if (path == null)
			throw new IllegalArgumentException("Path cannot be null");
		if (!path.startsWith("/"))
			throw new IllegalArgumentException("Path must begin with /");
		if (path.contains(":"))
			throw new IllegalArgumentException("Path must not contain : character");

		String[] components = path.trim().split("/");

		pathList = new ArrayList<>();

		for (String string : components) {

			if (string == null || string.isEmpty() || string.equals("/"))
				continue;
			pathList.add(string);
		}
	}

	/**
	 * Returns an iterator over the components of the path.
	 * 
	 * <p>
	 * The iterator cannot be used to modify the path object - the
	 * <code>remove</code> method is not supported.
	 * 
	 * @return The iterator.
	 */
	@Override
	public Iterator<String> iterator() {
		// throw new UnsupportedOperationException("not implemented");

		PathListIterator pathListIterator = new PathListIterator(pathList.iterator());
		return (pathListIterator);
	}

	/**
	 * Lists the paths of all files in a directory tree on the local filesystem.
	 * 
	 * @param directory The root directory of the directory tree.
	 * @return An array of relative paths, one for each file in the directory tree.
	 * @throws FileNotFoundException    If the root directory does not exist.
	 * @throws IllegalArgumentException If <code>directory</code> exists but does
	 *                                  not refer to a directory.
	 */
	public static Path[] list(File directory) throws FileNotFoundException {
		// throw new UnsupportedOperationException("not implemented");

		if (directory == null) {
			throw new NullPointerException("directory input is null");

		}

		if (!directory.isDirectory()) {
			throw new IllegalArgumentException();
		}

		if (!directory.exists())
			throw new FileNotFoundException();

		Path[] relativePaths = null;
		String rootPath = "";
		ArrayList<Path> allFiles = listAllFiles(directory, rootPath);

		relativePaths = new Path[allFiles.size()];

		int i = 0;
		for (Path p : allFiles) {
			relativePaths[i++] = p;
		}

		return relativePaths;
	}

	private static ArrayList<Path> listAllFiles(File directory, String prevPath) {

		ArrayList<Path> result = new ArrayList<Path>();

		for (File output : directory.listFiles()) {

			if (output.isFile()) {

				result.add(new Path(prevPath + "/" + output.getName()));
			} else if (output.isDirectory()) {

				ArrayList<Path> sub = listAllFiles(output, prevPath + "/" + output.getName());

				for (Path p : sub)
					result.add(p);
			}
		}

		return result;
	}

	/**
	 * Determines whether the path represents the root directory.
	 * 
	 * @return <code>true</code> if the path does represent the root directory, and
	 *         <code>false</code> if it does not.
	 */
	public boolean isRoot() {
		//throw new UnsupportedOperationException("not implemented");
		
		return pathList.isEmpty();
	}

	/**
	 * Returns the path to the parent of this path.
	 * 
	 * @throws IllegalArgumentException If the path represents the root directory,
	 *                                  and therefore has no parent.
	 */
	public Path parent() {
		//throw new UnsupportedOperationException("not implemented");
		
		if(this.isRoot())
			throw new IllegalArgumentException("No parent since this is the root directory ");
		
		Path result = new Path();
		for(String paths : pathList) {
			result.pathList.add(paths);
		}
		
		result.pathList.remove(result.pathList.size() - 1);
		
		return result;
	}

	/**
	 * Returns the last component in the path.
	 * 
	 * @throws IllegalArgumentException If the path represents the root directory,
	 *                                  and therefore has no last component.
	 */
	public String last() {
		//throw new UnsupportedOperationException("not implemented");
		
		if(pathList.isEmpty())
			throw new IllegalArgumentException("Root directory has no last.");
		
		String lastComponent = pathList.get(pathList.size() - 1);
		
		return lastComponent; 
	}

	/**
	 * Determines if the given path is a subpath of this path.
	 * 
	 * <p>
	 * The other path is a subpath of this path if it is a prefix of this path. Note
	 * that by this definition, each path is a subpath of itself.
	 * 
	 * @param other The path to be tested.
	 * @return <code>true</code> If and only if the other path is a subpath of this
	 *         path.
	 */
	public boolean isSubpath(Path other) {
		//throw new UnsupportedOperationException("not implemented");
		
		// some basic checks 
		if(other == null)
			throw new NullPointerException("other path is null");
		
		if(this == other)
			return true; 
		
		if(other.pathList.size() > this.pathList.size())
			return false; 
		
		int index = 0; 
		while(index < other.pathList.size()) {
			
			if(other.pathList.get(index).equals(pathList.get(index))) {
				index++;
				continue;
			}
			else 
				return false; 
			
		}
		return true; 
	}

	/**
	 * Converts the path to <code>File</code> object.
	 * 
	 * @param root The resulting <code>File</code> object is created relative to
	 *             this directory.
	 * @return The <code>File</code> object.
	 */
	public File toFile(File root) {
		// throw new UnsupportedOperationException("not implemented");
		
		String fileName = this.toString();
		return new File(root, fileName);
	}

	/**
	 * Compares this path to another.
	 * 
	 * <p>
	 * An ordering upon <code>Path</code> objects is provided to prevent deadlocks
	 * between applications that need to lock multiple filesystem objects
	 * simultaneously. By convention, paths that need to be locked simultaneously
	 * are locked in increasing order.
	 * 
	 * <p>
	 * Because locking a path requires locking every component along the path, the
	 * order is not arbitrary. For example, suppose the paths were ordered first by
	 * length, so that <code>/etc</code> precedes <code>/bin/cat</code>, which
	 * precedes <code>/etc/dfs/conf.txt</code>.
	 * 
	 * <p>
	 * Now, suppose two users are running two applications, such as two instances of
	 * <code>cp</code>. One needs to work with <code>/etc</code> and
	 * <code>/bin/cat</code>, and the other with <code>/bin/cat</code> and
	 * <code>/etc/dfs/conf.txt</code>.
	 * 
	 * <p>
	 * Then, if both applications follow the convention and lock paths in increasing
	 * order, the following situation can occur: the first application locks
	 * <code>/etc</code>. The second application locks <code>/bin/cat</code>. The
	 * first application tries to lock <code>/bin/cat</code> also, but gets blocked
	 * because the second application holds the lock. Now, the second application
	 * tries to lock <code>/etc/dfs/conf.txt</code>, and also gets blocked, because
	 * it would need to acquire the lock for <code>/etc</code> to do so. The two
	 * applications are now deadlocked.
	 * 
	 * @param other The other path.
	 * @return Zero if the two paths are equal, a negative number if this path
	 *         precedes the other path, or a positive number if this path follows
	 *         the other path.
	 */
	@Override
	public int compareTo(Path other) {
		//throw new UnsupportedOperationException("not implemented");
		return this.toString().compareTo(other.toString());
	}

	/**
	 * Compares two paths for equality.
	 * 
	 * <p>
	 * Two paths are equal if they share all the same components.
	 * 
	 * @param other The other path.
	 * @return <code>true</code> if and only if the two paths are equal.
	 */
	@Override
	public boolean equals(Object other) {
		
		return this.toString().equals(other.toString());
	}

	/** Returns the hash code of the path. */
	@Override
	public int hashCode() {
		
		return this.toString().hashCode();
	}

	/**
	 * Converts the path to a string.
	 * 
	 * <p>
	 * The string may later be used as an argument to the <code>Path(String)</code>
	 * constructor.
	 * 
	 * @return The string representation of the path.
	 */
	@Override
	public String toString() {
		
		if(this.isRoot())
			return "/";
		
		StringBuilder stringPath = new StringBuilder(); 
		
		for(String p : pathList) {
			stringPath.append("/").append(p);
		}
		return stringPath.toString();
	}
}
