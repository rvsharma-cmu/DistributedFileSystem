package storage;

import java.io.*;
import java.net.*;

import common.*;
import rmi.*;
import naming.*;

/** Storage server.

    <p>
    Storage servers respond to client file access requests. The files accessible
    through a storage server are those accessible under a given directory of the
    local filesystem.
 */
public class StorageServer implements Storage, Command {
	
	private File root; 
	private int clientPort; 
	private int commandPort; 
	private Skeleton<Storage> storageSkeleton; 
	private Skeleton<Command> commandSkeleton; 
	private InetSocketAddress clientAddr; 
	private InetSocketAddress commandAddr; 
	
    /** Creates a storage server, given a directory on the local filesystem, and
        ports to use for the client and command interfaces.

        <p>
        The ports may have to be specified if the storage server is running
        behind a firewall, and specific ports are open.

        @param root Directory on the local filesystem. The contents of this
                    directory will be accessible through the storage server.
        @param client_port Port to use for the client interface, or zero if the
                           system should decide the port.
        @param command_port Port to use for the command interface, or zero if
                            the system should decide the port.
        @throws NullPointerException If <code>root</code> is <code>null</code>.
    */
    public StorageServer(File root, int client_port, int command_port)
    {
        if(root == null)
        	throw new NullPointerException("root is null");
        
        this.root = root; 
        this.clientPort = client_port;
        this.commandPort = command_port;
    }

    /** Creats a storage server, given a directory on the local filesystem.

        <p>
        This constructor is equivalent to
        <code>StorageServer(root, 0, 0)</code>. The system picks the ports on
        which the interfaces are made available.

        @param root Directory on the local filesystem. The contents of this
                    directory will be accessible through the storage server.
        @throws NullPointerException If <code>root</code> is <code>null</code>.
     */
    public StorageServer(File root)
    {
        this(root, 0, 0);
    }

    /** Starts the storage server and registers it with the given naming
        server.

        @param hostname The externally-routable hostname of the local host on
                        which the storage server is running. This is used to
                        ensure that the stub which is provided to the naming
                        server by the <code>start</code> method carries the
                        externally visible hostname or address of this storage
                        server.
        @param naming_server Remote interface for the naming server with which
                             the storage server is to register.
        @throws UnknownHostException If a stub cannot be created for the storage
                                     server because a valid address has not been
                                     assigned.
        @throws FileNotFoundException If the directory with which the server was
                                      created does not exist or is in fact a
                                      file.
        @throws RMIException If the storage server cannot be started, or if it
                             cannot be registered.
     */
    public synchronized void start(String hostname, Registration naming_server)
        throws RMIException, UnknownHostException, FileNotFoundException
    {
        // throw new UnsupportedOperationException("not implemented");
    	
    	Storage storage; 
    	Command command; 
    	
    	clientAddr = new InetSocketAddress(hostname, clientPort);
    	commandAddr = new InetSocketAddress(hostname, commandPort);
    	
    	storageSkeleton = new Skeleton<Storage>(Storage.class, this, clientAddr);
    	commandSkeleton = new Skeleton<Command>(Command.class, this, commandAddr);
    	
    	storageSkeleton.start();
    	commandSkeleton.start();
    	
    	storage = Stub.create(Storage.class, storageSkeleton);
		command = Stub.create(Command.class, commandSkeleton);
		
		Path[] duplicates = naming_server.register(storage, command, Path.list(root));
		storageDeDup(duplicates);
		
    	
    }

    private void storageDeDup(Path[] duplicates) {
		// TODO Auto-generated method stub
    	
    	for(Path path : duplicates) {
    		
    		File duplicateFile = new File(root + path.toString());
    		if(duplicateFile.delete()) {
    			
    			Path parent = path.parent();
    			
    			while(!parent.isRoot()) {
    				
    				File file = new File(root + parent.toString());
    				if(file.list().length == 0)
    					file.delete();
    				else 
    					break;
    				parent = parent.parent();
    			}
    		}
    	}
		
	}

	/** Stops the storage server.

        <p>
        The server should not be restarted.
     */
    public void stop()
    {
        // throw new UnsupportedOperationException("not implemented");
    	
    	storageSkeleton.stop();
    	commandSkeleton.stop();
    }

    /** Called when the storage server has shut down.

        @param cause The cause for the shutdown, if any, or <code>null</code> if
                     the server was shut down by the user's request.
     */
    protected void stopped(Throwable cause)
    {
    	//do nothing!
    }

    // The following methods are documented in Storage.java.
    @Override
    public synchronized long size(Path file) throws FileNotFoundException
    {
        // throw new UnsupportedOperationException("not implemented");
    	
    	File fileName = new File (root + file.toString());
    	
    	if(! fileCheck(fileName))
    		throw new FileNotFoundException(file.toString() + "was not found");
    	
    	FileInputStream fstream = new FileInputStream(fileName);
    	
    	try {
    		
    		long sizeAvailable = fstream.available();
    		fstream.close();
			return sizeAvailable;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			try {
				fstream.close();
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			throw new FileNotFoundException();
		}
    	
    }

    @Override
    public synchronized byte[] read(Path file, long offset, int length)
        throws FileNotFoundException, IOException
    {
        // throw new UnsupportedOperationException("not implemented");
    	
    	File fileName = file.toFile(root);
    	
    	if(offset < 0 || length < 0)
    		throw new IndexOutOfBoundsException();
    	
    	if(! fileCheck(fileName))
    		throw new FileNotFoundException(file.toString() + "was not found");
    	
    	FileInputStream fstream = new FileInputStream(fileName);

    	if(fstream.available() >= offset + length) {
    		
    		byte[] read = new byte[length];
    		
    		fstream.read(read, (int) offset, length);
    		
    		fstream.close();
    		return read; 
    		
    	} else {
    		fstream.close();
    		throw new IndexOutOfBoundsException();
    	}
    }

    @Override
    public synchronized void write(Path file, long offset, byte[] data) throws FileNotFoundException, IOException
    {
    	File fileName = file.toFile(root);
    	if(data == null)
    		throw new NullPointerException(); 
    	
    	if(offset < 0)
    		throw new IndexOutOfBoundsException();
    	
    	if(! fileCheck(fileName))
				throw new FileNotFoundException("file was not found");
    	boolean append = false; 
    	FileOutputStream outputStream = null; 
    	
    	try {
			long size = size(file);
			
			if(size >= offset) {
				
				outputStream = new FileOutputStream(fileName);
				outputStream.write(data, (int)offset, data.length);
				
			} else {
				append = true; 
				outputStream = new FileOutputStream(fileName, append);
				
				for(int i = (int) size; i < offset; i++) {
					outputStream.write(0);
				}
				outputStream.write(data);
			}
		} catch (FileNotFoundException e) {
			System.out.println("File output stream not found");
			e.printStackTrace();
		} catch (IOException e) {
			System.out.println("Exception while writing block to file stream");
			e.printStackTrace();
		}
    	
    	try {
			outputStream.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
    }

    // The following methods are documented in Command.java.
    @Override
    public synchronized boolean create(Path file)
    {
    	if(file == null)
    		throw new NullPointerException("Path file cannot be null");
    	
    	if(file.isRoot()) {
    		System.out.println("Path file cannot be root directory ");
    		return false;
    	}
    	File fileName = file.toFile(root);
    	if(fileName.exists()) {
    		return false;
    	}
    	
    	fileName.getParentFile().mkdirs();
    	
    	try {
			fileName.createNewFile();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println("Caught IOException while creating directory");
			e.printStackTrace();
			return false; 
		}
    		
    	return true;
    }

    @Override
    public synchronized boolean delete(Path path)
    {
        if(path == null)
        	throw new NullPointerException("Path is null");
        if(path.isRoot())
        	return false; 
        
        File file = path.toFile(root);
        
        // if file does not exists return false 
        if( ! file.exists()) {
        	System.out.println("File does not exists ");
        	return false; 
        }
        
        if(file.isFile()) {
        	file.delete();
        }
        boolean deleted = false; 
        if(file.isDirectory()) {
        	
        	if(file.list().length == 0)
        		file.delete();
        	else {
	        	String[] files = file.list();
	        	for(String pathName : files) {
	        		if(this.delete(new Path(path, pathName)));
	        			deleted = true;
	        	}
	        	if(file.list().length == 0)
	        		file.delete();
        	}
        }
        else
        {
        	file.delete();
        }
        	
        	
        return deleted; 
    }

    @Override
    public synchronized boolean copy(Path file, Storage server)
        throws RMIException, FileNotFoundException, IOException
    {
        if(server == null || file == null)
        	throw new NullPointerException();
        boolean copied = false; 
        byte[] read = server.read(file, 0, (int)server.size(file));
        
        if(create(file)) {
        	write(file, 0, read);
        	copied = true; 
        }
        
        else 
        	System.out.println("Error in creating and copying out the file" + file.toString());
        
        return copied; 
    }

	public InetSocketAddress getClientAddr() {
		return clientAddr;
	}

	public void setClientAddr(InetSocketAddress clientAddr) {
		this.clientAddr = clientAddr;
	}

	public InetSocketAddress getCommandAddr() {
		return commandAddr;
	}

	public void setCommandAddr(InetSocketAddress commandAddr) {
		this.commandAddr = commandAddr;
	}
	
	public boolean fileCheck(File file) {
		
		return (file.exists() && file.isFile());
			
	}
}
