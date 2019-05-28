# Brief
obsutil is a command line tool for accessing Object Storage Service (OBS). You can use this tool to perform common configurations in OBS, such as creating buckets, uploading and downloading files/folders, and deleting files/folders. If you are familiar with command line interface (CLI), obsutil is recommended as an optimal tool for batch processing and automated tasks.

# Compile
1. Download the code to your local PC (for example, the /xxx/obsutil directory) and prepare the Go development environment.

2. Modify the src/command/config.go file. Change the values of aesKey and aesIv to customized private key character strings. (Note: The length of a customized character string must be 16 characters.)

3. Set the parent directory of src to the path of GO_PATH. For example, export GOPATH=/xxx/obsutil.

4. After setting the compilation parameters for your operating system, run the go install obsutil command to perform compilation.


# Download
Windows: https://github.com/chewaiwai/obsutil/raw/master/release/obsutil_windows_amd64.zip

Linux: https://github.com/chewaiwai/obsutil/raw/master/release/obsutil_linux_amd64.tar.gz

MacOS: https://github.com/chewaiwai/obsutil/raw/master/release/obsutil_darwin_amd64.tar.gz

# Quick Start

Usage: obsutil [command] [args...] [options...]

You can use "obsutil help command" to view the specific help of each command

Basic commands:

    abort   cloud_url [options...]
            abort multipart uploads       

    chattri cloud_url [options...]        
            set bucket or object properties
          
    cp      file_url cloud_url [options...]
            cloud_url file_url [options...]
            cloud_url cloud_url [options...]
            upload, download or copy objects
          
    ls      [cloud_url] [options...]      
            list buckets or objects/multipart uploads in a bucket

    mb      cloud_url [options...]        
            create a bucket with the specified parameters

    mkdir   cloud_url|folder_url          
            create folder(s) in a specified bucket or in the local file system

    mv      cloud_url cloud_url [options...]
            move objects                  

    restore cloud_url [options...]        
            restore objects in a bucket to be readable

    rm      cloud_url [options...]        
            delete a bucket or objects in a bucket

    sign    cloud_url [options...]        
            generate the download url(s) for the objects in a specified bucket

    stat    cloud_url                     
            show the properties of a bucket or an object

    sync    file_url cloud_url [options...]
            cloud_url file_url [options...]
            cloud_url cloud_url [options...]
            synchronize objects from the source to the destination

Other commands:

    archive [archive_url]                 
            archive log files to local file system or OBS

    clear   [checkpoint_dir] [options...] 
            delete part records           

    config  [options...]                  
            update the configuration file 

    help    [command]                     
            view command help information 

    version show version      
