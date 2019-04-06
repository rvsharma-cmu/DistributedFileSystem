package naming;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;

import rmi.*;
import common.*;
import storage.*;

/** Naming server.

    <p>
    Each instance of the filesystem is centered on a single naming server. The
    naming server maintains the filesystem directory tree. It does not store any
    file data - this is done by separate storage servers. The primary purpose of
    the naming server is to map each file name (path) to the storage server
    which hosts the file's contents.

    <p>
    The naming server provides two interfaces, <code>Service</code> and
    <code>Registration</code>, which are accessible through RMI. Storage servers
    use the <code>Registration</code> interface to inform the naming server of
    their existence. Clients use the <code>Service</code> interface to perform
    most filesystem operations. The documentation accompanying these interfaces
    provides details on the methods supported.

    <p>
    Stubs for accessing the naming server must typically be created by directly
    specifying the remote network address. To make this possible, the client and
    registration interfaces are available at well-known ports defined in
    <code>NamingStubs</code>.
 */
public class NamingServer implements Service, Registration
{
	
	private ArrayList<StorageInfo> storageServers; 
	
	private Map<Path, Paths> fileDirectory; 
	
	private Skeleton<Service> serviceSkeleton; 
	private Skeleton<Registration> registrationSkeleton; 
	
	private Map<Storage, Command> storageConnections; 
	
    /** Creates the naming server object.

        <p>
        The naming server is not started.
     */
    public NamingServer()
    {
    	boolean value = false; 
    	
    	storageServers = new ArrayList<StorageInfo>(); 
    	fileDirectory = new ConcurrentHashMap<Path, Paths>(); 
    	storageConnections = new ConcurrentHashMap<Storage, Command>(); 
    	fileDirectory.put(new Path(), new Paths(value));
    }

    /** Starts the naming server.

        <p>
        After this method is called, it is possible to access the client and
        registration interfaces of the naming server remotely.

        @throws RMIException If either of the two skeletons, for the client or
                             registration server interfaces, could not be
                             started. The user should not attempt to start the
                             server again if an exception occurs.
     */
    public synchronized void start() throws RMIException
    {
        // throw new UnsupportedOperationException("not implemented");
    	InetSocketAddress registrationAddress = new InetSocketAddress(NamingStubs.REGISTRATION_PORT);
    	InetSocketAddress serviceAddress = new InetSocketAddress(NamingStubs.SERVICE_PORT);
    	
    	registrationSkeleton = new Skeleton<Registration>(Registration.class, this, registrationAddress);
    	serviceSkeleton = new Skeleton<Service>(Service.class, this, serviceAddress);
    	
    	registrationSkeleton.start();
    	serviceSkeleton.start();
    }

    /** Stops the naming server.

        <p>
        This method commands both the client and registration interface
        skeletons to stop. It attempts to interrupt as many of the threads that
        are executing naming server code as possible. After this method is
        called, the naming server is no longer accessible remotely. The naming
        server should not be restarted.
     */
    public void stop()
    {
        // throw new UnsupportedOperationException("not implemented");
    	
    	serviceSkeleton.stop();
    	registrationSkeleton.stop();
    	stopped(null);
    	
    }

    /** Indicates that the server has completely shut down.

        <p>
        This method should be overridden for error reporting and application
        exit purposes. The default implementation does nothing.

        @param cause The cause for the shutdown, or <code>null</code> if the
                     shutdown was by explicit user request.
     */
    protected void stopped(Throwable cause)
    {
    }

    // The following public methods are documented in Service.java.
    @Override
    public void lock(Path path, boolean exclusive) throws FileNotFoundException
    {
        //throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public void unlock(Path path, boolean exclusive)
    {
        //throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public boolean isDirectory(Path path) throws FileNotFoundException
    {
        // throw new UnsupportedOperationException("not implemented");
    	if(fileDirectory.containsKey(path)) {
    		return !fileDirectory.get(path).flag;
    	} else {
    		throw new FileNotFoundException();
    	}
    }

    @Override
    public String[] list(Path directory) throws FileNotFoundException
    {
        // throw new UnsupportedOperationException("not implemented");
    	
    	if(!fileDirectory.containsKey(directory) || fileDirectory.get(directory).flag)
    		throw new FileNotFoundException();
    	
    	int index = 0; 
    	HashSet<Path> children = fileDirectory.get(directory).decPaths; 
    	
    	String[] output = new String[children.size()]; 
    	for(Path p: children) {
    		output[index++] = p.last();
    	}
    	
    	return output; 
    }

    @Override
    public boolean createFile(Path file)
        throws RMIException, FileNotFoundException
    {
        // throw new UnsupportedOperationException("not implemented");

    	if(!fileDirectory.containsKey(file.parent()) || fileDirectory.get(file).flag) { 
    		throw new FileNotFoundException(); 
    	}
    	
    	if(storageConnections.isEmpty())
    		throw new IllegalStateException();
    	
    	if(fileDirectory.containsKey(file))
    		return false; 
    	
    	int index = ThreadLocalRandom.current().nextInt(storageServers.size());
    	Storage storage = storageServers.get(index).storageStub;
    	Command command = storageConnections.get(storage);
    	if(command.create(file)) {
    		fileDirectory.put(file, new Paths(true));
    		StorageInfo storageInfo = new StorageInfo(storage, command);
    		
    		fileDirectory.get(file).storageServers.add(storageInfo);
    		addParentToStorage(file, storageInfo);
    		return true; 
    	} else 
    		return false; 	
    }

    private void addParentToStorage(Path file, StorageInfo storageInfo) {
		
    	Path path = file.parent(); 
    	
    	while(! path.isRoot()) {
    		path = file.parent();
    		if(! fileDirectory.containsKey(path))
    			fileDirectory.put(path, new Paths(false));
    		fileDirectory.get(path).decPaths.add(file);
    		fileDirectory.get(path).storageServers.add(storageInfo);
    		file = path;
    	} 
	}

	@Override
    public boolean createDirectory(Path directory) throws FileNotFoundException
    {
        // throw new UnsupportedOperationException("not implemented");
		
		if(fileDirectory.containsKey(directory))
			return false; 
		
		Path p = directory.parent(); 
		if(!this.fileDirectory.containsKey(p) || 
				fileDirectory.get(p).flag )
			throw new FileNotFoundException();
		
		fileDirectory.put(directory, new Paths(false));
		
		addParentPath(directory);
		return true; 
    }

    private void addParentPath(Path directory) {
		
    	Path path = directory.parent(); 
    	
    	while(! path.isRoot()) {
    		if(! fileDirectory.containsKey(path)) {
    			fileDirectory.put(path, new Paths(false));
    			
    		} else {
    			fileDirectory.get(path).decPaths.add(directory); 
    			break; 
    		}
    		fileDirectory.get(path).decPaths.add(directory);
    		directory = path; 
    	}
		
	}

	@Override
    public boolean delete(Path path) throws FileNotFoundException
    {
        //throw new UnsupportedOperationException("not implemented");
		
		if(! this.fileDirectory.containsKey(path.parent()) || 
				! this.fileDirectory.containsKey(path))
			throw new FileNotFoundException();
		Paths paths = fileDirectory.get(path);
		Path parentPath = path.parent();
		HashSet<StorageInfo> storageInfo = fileDirectory.get(path).storageServers;
		
		for(StorageInfo storage : storageInfo) {
			
			try {
				if(storage.commandStub.delete(path))
					continue; 
				else 
					return false;
			} catch (RMIException e) {
				// TODO Auto-generated catch block
				System.out.println("Call cannot be completed due to a network error ");
				e.printStackTrace();
			} 
		}
		fileDirectory.remove(path);
		fileDirectory.get(parentPath).decPaths.remove(path);
		return true;
    }

    @Override
    public Storage getStorage(Path file) throws FileNotFoundException
    {
        // throw new UnsupportedOperationException("not implemented");
    	
    	if(! this.fileDirectory.containsKey(file) || 
    			! this.fileDirectory.get(file).flag) {
    		throw new FileNotFoundException();
    	}
    	
    	HashSet<StorageInfo> storageConn = fileDirectory.get(file).storageServers;
    	
    	Iterator<StorageInfo> i = storageConn.iterator();
    	
    	int index = ThreadLocalRandom.current().nextInt(storageConn.size());
    	while(index > 0) {
    		i.next();
    		index--;
    	}
    	
    	return i.next().storageStub;
    }

    // The method register is documented in Registration.java.
    @Override
    public Path[] register(Storage client_stub, Command command_stub,
                           Path[] files)
    {
        // throw new UnsupportedOperationException("not implemented");
    	
    	if(files == null || client_stub == null || command_stub == null) {
    		throw new NullPointerException();
    	}
    	
    	if(storageConnections.containsKey(client_stub))
    		throw new IllegalStateException();
    	Path[] output = null; 
    	StorageInfo storage = new StorageInfo(client_stub, command_stub);
    	storageConnections.put(client_stub, command_stub);
    	storageServers.add(storage);
    	
    	List<Path> fileNames = new ArrayList<>(); 
    	
    	for(Path eachPath : files) {
    		
    		if(!eachPath.isRoot()) {
    			
    			if(! fileDirectory.containsKey(eachPath)) {
    				fileDirectory.put(eachPath, new Paths(true));
    				fileDirectory.get(eachPath).storageServers.add(storage);
    				addParentToStorage(eachPath, storage);
    				
    			} else {
    				fileNames.add(eachPath);
    			}
    		}
    	}
    	output = new Path[fileNames.size()];
    	
    	return fileNames.toArray(output);  
    }
    
    private class Paths {
    	
    	HashSet<StorageInfo> storageServers;  
    	HashSet<Path> decPaths; 
    	int numberOfReads; 
    	
    	boolean flag; 
    	
    	public Paths(boolean value) {
    		flag = value; 
    		numberOfReads = 0; 
    		decPaths = new HashSet<Path>(); 
    		storageServers = new HashSet<StorageInfo>(); 
    	}
    	
    }
    
    private class StorageInfo {
    	
    	Command commandStub; 
    	Storage storageStub; 
    	
    	public StorageInfo(Storage storage, Command command) {
    		commandStub = command; 
    		storageStub = storage; 
    	}
    	
    }
}
