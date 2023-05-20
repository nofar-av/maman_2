from typing import List
import Utility.DBConnector as Connector
from Utility.DBConnector import ResultSet
from Utility.ReturnValue import ReturnValue
from Utility.Exceptions import DatabaseException
from Business.Photo import Photo
from Business.RAM import RAM
from Business.Disk import Disk
from psycopg2 import sql

def executeQuery(query) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute(query)
        conn.commit()
    except DatabaseException.ConnectionInvalid as e:
        print(e)
        return ReturnValue.ERROR
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
        return ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
        return ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
        return ReturnValue.ALREADY_EXISTS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
        return ReturnValue.NOT_EXISTS
    except Exception as e:
        print(e)
        return ReturnValue.ERROR
    finally:
        # will happen any way after try termination or exception handling
        conn.close()
        return ReturnValue.OK


def createTables()-> None:
    create_photos_table = """
    CREATE TABLE Photos 
    (PhotoId INTEGER,
     Description TEXT,
     DiskSizeNeeded INTEGER NOT NULL,
     CHECK (PhotoId > 0),
     CHECK (DiskSizeNeeded >= 0),
     PRIMARY KEY (PhotoId));
     """
    create_disks_table = """
    CREATE TABLE Disks
    (DiskId INTEGER,
     ManufacturingCompany TEXT NOT NULL,
     Speed INTEGER NOT NULL,
     FreeSpace INTEGER NOT NULL,
     CostPerByte INTEGER NOT NULL,
     PRIMARY KEY (DiskId),
     CHECK (DiskId > 0),
     CHECK (Speed > 0),
     CHECK (CostPerByte > 0),
     CHECK(FreeSpace >= 0));
     """
    create_rams_table = """
     CREATE TABLE RAMs
    (RAMId INTEGER,
     Size INTEGER NOT NULL,
     Company TEXT NOT NULL,
     PRIMARY KEY (RAMId),
     CHECK (RAMId > 0),
     CHECK (Size > 0));
     """
    create_photos_on_disk_table = """
    CREATE TABLE PhotosOnDisk 
    (DiskId INTEGER,
     PhotoId INTEGER,
     PRIMARY KEY (DiskId, PhotoId),
     FOREIGN KEY (PhotoId)  REFERENCES Photos ON DELETE CASCADE,
     FOREIGN KEY (DiskId) REFERENCES disks ON DELETE CASCADE);
    """
    create_RAM_on_disk_table = """
    CREATE TABLE RAMsOnDisk
    (DiskId INTEGER,
    RAMId Integer,
    PRIMARY KEY (DiskId, RAMId),
    FOREIGN KEY (DiskId) REFERENCES Disks ON DELETE CASCADE,
    FOREIGN KEY (RAMId) References RAMs ON DELETE CASCADE);
    """
    executeQuery(create_photos_table)
    executeQuery(create_disks_table)
    executeQuery(create_rams_table)
    executeQuery(create_photos_on_disk_table)
    executeQuery(create_RAM_on_disk_table)


def clearTables():
    clear_photos = "DELETE FROM Photos"
    clear_disks = "DELETE FROM Disks"
    clear_RAMs = "DELETE FROM RAMs"
    executeQuery(clear_photos)
    executeQuery(clear_disks)
    executeQuery(clear_RAMs)


def dropTables():
    drop_photos = "DROP TABLE IF EXISTS Photos CASCADE"
    drop_disks = "DROP TABLE IF EXISTS Disks CASCADE"
    drop_RAM = "DROP TABLE IF EXISTS RAMs CASCADE"
    drop_photos_on_disk = "DROP TABLE IF EXISTS PhotosOnDisk"
    drop_RAMs_on_disk = "DROP TABLE IF EXISTS RAMsOnDisk"
    executeQuery(drop_photos)
    executeQuery(drop_disks)
    executeQuery(drop_RAM)
    executeQuery(drop_photos_on_disk)
    executeQuery(drop_RAMs_on_disk)


def addPhoto(photo: Photo) -> ReturnValue:
    photo_query = sql.SQL("INSERT INTO Photos VALUES({ID}, {Description}, {Size})")\
        .format(ID=sql.Literal(photo.getPhotoID()),Description=sql.Literal(photo.getDescription()),
                Size=sql.Literal(photo.getSize()))
    return executeQuery(photo_query)


def getPhotoByID(photoID: int) -> Photo:
    conn = None
    rows_effected, result = 0, Photo()
    try:
        conn = Connector.DBConnector()
        rows_effected, result = conn.execute(f"SELECT * FROM Photos WHERE PhotoId = {photoID}")
    except Exception as e:
        return Photo.badPhoto()

    finally:
        conn.close()
        return result


def deletePhoto(photo: Photo) -> ReturnValue:
    del_photo_query = f"""
        BEGIN TRANSACTION;

        UPDATE Disks
        SET FreeSpace = FreeSpace + {photo.getSize()}
        WHERE DiskId = (SELECT DiskID FROM PhotosOnDisk
                        WHERE PhotoId = {photo.getPhotoID()});  
               
        DELETE FROM Photos 
        WHERE PhotoId = {photo.getPhotoID()};

        COMMIT;
        """

    return executeQuery(del_photo_query)
    # conn = None
    # rows_effected = 0
    # try:
    #     conn = Connector.DBConnector()
    #     rows_effected, _ = conn.execute(del_photo_query)
    # except Exception as e:
    #     return ReturnValue.ERROR
    #
    # finally:
    #     conn.close()
    #     return ReturnValue.OK #make sure that it's deleted from all tables


def addDisk(disk: Disk) -> ReturnValue:
    disk_query = sql.SQL("INSERT INTO Disks VALUES({ID}, {Manu}, {Speed}, {Space}, {Cost})") \
        .format(ID=sql.Literal(disk.getDiskID()), Manu=sql.Literal(disk.getCompany()),
                Speed=sql.Literal(disk.getSpeed()), Space=sql.Literal(disk.getFreeSpace()),
                Cost=sql.Literal(disk.getCost()))
    return executeQuery(disk_query)


def getDiskByID(diskID: int) -> Disk:
    conn = None
    rows_effected, result = 0, Disk()
    try:
        conn = Connector.DBConnector()
        rows_effected, result = conn.execute(f"SELECT * FROM Disks WHERE DiskId = {diskID}")
    except Exception as e:
        return Disk.badDisk()
    finally:
        conn.close()
        return result


def deleteDisk(diskID: int) -> ReturnValue:
    conn = None
    rows_effected = 0
    try:
        conn = Connector.DBConnector()
        rows_effected, _ = conn.execute(f"DELETE FROM Disks WHERE DiskId = {diskID}")
    except Exception as e:
        return ReturnValue.ERROR

    finally:
        conn.close()
        return ReturnValue.OK  # make sure that it's deleted from all tables


def addRAM(ram: RAM) -> ReturnValue:
    return ReturnValue.OK


def getRAMByID(ramID: int) -> RAM:
    return RAM()


def deleteRAM(ramID: int) -> ReturnValue:
    return ReturnValue.OK


def addDiskAndPhoto(disk: Disk, photo: Photo) -> ReturnValue:
    return ReturnValue.OK


def addPhotoToDisk(photo: Photo, diskID: int) -> ReturnValue:
    add_photo_to_disk_query = f"""
    INSERT INTO PhotosOnDisk VALUES({diskID}, {photo.getPhotoID()})
    WHERE EXISTS (SELECT * FROM Disks
        WHERE DiskId = {diskID} AND FreeSpace >= {photo.getSize()})            
        """
    return executeQuery(add_photo_to_disk_query)


def removePhotoFromDisk(photo: Photo, diskID: int) -> ReturnValue:

    return ReturnValue.OK


def addRAMToDisk(ramID: int, diskID: int) -> ReturnValue:
    return ReturnValue.OK


def removeRAMFromDisk(ramID: int, diskID: int) -> ReturnValue:
    return ReturnValue.OK


def averagePhotosSizeOnDisk(diskID: int) -> float:
    """CREATE VIEW PhotosOnDiscSize AS
     SELECT Photo_ID, Disk_ID, Photo_SIZE
    FROM PhotosonDisk, Photos
    Where Photos.PhotoID == PhotosonDisk.PhotoID
    """



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


if __name__ == '__main__':
    dropTables()
    print("Hello")
    print("0. Creating all tables")
    createTables()
    addPhoto(Photo(1, "Nofar", 50))
    our_photo = Photo(getPhotoByID(1))
    addDisk(Disk(1,"Michal", 50, 51, 20))
    addPhotoToDisk(our_photo, 1)
    deletePhoto(our_photo)
    dropTables()

