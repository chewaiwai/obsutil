# obsutil
obsutil is a command line tool for accessing Object Storage Service (OBS). You can use this tool to perform common configurations in OBS, such as creating buckets, uploading and downloading files/folders, and deleting files/folders. If you are familiar with command line interface (CLI), obsutil is recommended as an optimal tool for batch processing and automated tasks.


# quickstart

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
