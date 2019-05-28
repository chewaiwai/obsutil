

package obs

import ()

type IFSClient interface {
	NewBucket(input *NewBucketInput) (output *BaseModel, err error)

	GetBucketFSStatus(input *GetBucketFSStatusInput) (output *GetBucketFSStatusOutput, err error)

	GetAttribute(input *GetAttributeInput) (output *GetAttributeOutput, err error)

	DropFile(input *DropFileInput) (output *DropFileOutput, err error)

	NewFolder(input *NewFolderInput) (output *NewFolderOutput, err error)

	NewFile(input *NewFileInput) (output *NewFileOutput, err error)

	RenameFile(input *RenameFileInput) (output *RenameFileOutput, err error)

	RenameFolder(input *RenameFolderInput) (output *RenameFolderOutput, err error)

	Close()
}
