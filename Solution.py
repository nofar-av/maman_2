from typing import List
import Utility.DBConnector as Connector
from Utility.ReturnValue import ReturnValue
from Utility.Exceptions import DatabaseException
from Business.Photo import Photo
from Business.RAM import RAM
from Business.Disk import Disk
from psycopg2 import sql


def createTables():
    CREATE TABLE Photos 
    (PhotoId INTEGER,
     Description TEXT,
     DiskSizeNeeded INTEGER NOT NULL,
     CHECK (PhotoId > 0),
     CHECK (DiskSizeNeeded >= 0),
     UNIQUE / PRIMARY KEY (PhotoId));
    
    CREATE TABLE Disks
    (DiskId INTEGER,
     ManufacturingCompany TEXT NOT NULL,
     Speed INTEGER NOT NULL,
     FreeSpace INTEGER NOT NULL,
     CostPerByte INTEGRER NOT NULL,
     UNIQUE / PRIMARY KEY (DiskId),
     CHECK (DiskId > 0),
     CHECK (Speed > 0),
     CHECK (CostPerByte > 0),
     CHECK(FreeSpace >= 0));
     
     CREATE TABLE RAMS
    (RAMId INTEGER,
     Size INTEGER NOT NULL,
     Company TEXT NOT NULL,
     UNIQUE / PRIMARY KEY (RAMId),
      CHECK (DiskId > 0),
     CHECK (Speed > 0));
    
    CREATE TABLE DiskAndStuff 
    (DiskId INTEGER,
     PhotoId INTEGER,
     FOREIGN KEY (PhotoId)  REFERENCES Photos,
     PRIMARY KEY (DiskId, PhotoId));



def clearTables():
    pass


def dropTables():
    pass


def addPhoto(photo: Photo) -> ReturnValue:
    return ReturnValue.OK


def getPhotoByID(photoID: int) -> Photo:
    return Photo()


def deletePhoto(photo: Photo) -> ReturnValue:
    return ReturnValue.OK


def addDisk(disk: Disk) -> ReturnValue:
    return ReturnValue.OK


def getDiskByID(diskID: int) -> Disk:
    return Disk()


def deleteDisk(diskID: int) -> ReturnValue:
    return ReturnValue.OK


def addRAM(ram: RAM) -> ReturnValue:
    return ReturnValue.OK


def getRAMByID(ramID: int) -> RAM:
    return RAM()


def deleteRAM(ramID: int) -> ReturnValue:
    return ReturnValue.OK


def addDiskAndPhoto(disk: Disk, photo: Photo) -> ReturnValue:
    return ReturnValue.OK


def addPhotoToDisk(photo: Photo, diskID: int) -> ReturnValue:
    return ReturnValue.OK


def removePhotoFromDisk(photo: Photo, diskID: int) -> ReturnValue:
    return ReturnValue.OK


def addRAMToDisk(ramID: int, diskID: int) -> ReturnValue:
    return ReturnValue.OK


def removeRAMFromDisk(ramID: int, diskID: int) -> ReturnValue:
    return ReturnValue.OK


def averagePhotosSizeOnDisk(diskID: int) -> float:
    return 0


def getTotalRamOnDisk(diskID: int) -> int:
    return 0


def getCostForDescription(description: str) -> int:
    return 0


def getPhotosCanBeAddedToDisk(diskID: int) -> List[int]:
    return []


def getPhotosCanBeAddedToDiskAndRAM(diskID: int) -> List[int]:
    return []


def isCompanyExclusive(diskID: int) -> bool:
    return True


def isDiskContainingAtLeastNumExists(description : str, num : int) -> bool:
    return True


def getDisksContainingTheMostData() -> List[int]:
    return []


def getConflictingDisks() -> List[int]:
    return []


def mostAvailableDisks() -> List[int]:
    return []


def getClosePhotos(photoID: int) -> List[int]:
    return []
