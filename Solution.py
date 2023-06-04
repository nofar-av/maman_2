from typing import List
import Utility.DBConnector as Connector
from Utility.DBConnector import ResultSet
from Utility.ReturnValue import ReturnValue
from Utility.Exceptions import DatabaseException
from Business.Photo import Photo
from Business.RAM import RAM
from Business.Disk import Disk
from psycopg2 import sql



def CreatePhotoFromResultSet(result_set: ResultSet, rows_affected: int) -> Photo:
    new_photo = Photo.badPhoto()
    if (result_set is not None) and (rows_affected > 0):
        new_photo.setPhotoID(result_set.rows[0][0])
        new_photo.setDescription(result_set.rows[0][1])
        new_photo.setSize(result_set.rows[0][2])
    return new_photo


def executeQuery(query) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute(query)
        conn.commit()
    except DatabaseException.ConnectionInvalid as e:
        print(e)
        conn.rollback()
        return ReturnValue.ERROR
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
        conn.rollback()
        return ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
        conn.rollback()
        return ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
        conn.rollback()
        return ReturnValue.ALREADY_EXISTS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
        conn.rollback()
        return ReturnValue.NOT_EXISTS
    except Exception as e:
        print(e)
        conn.rollback()
        return ReturnValue.ERROR
    finally:
        # will happen any way after try termination or exception handling
        conn.close()
        return ReturnValue.OK


def createTables()-> None:
    create_photos_table = """
    CREATE TABLE Photos 
    (PhotoID INTEGER,
     Description TEXT,
     DiskSizeNeeded INTEGER NOT NULL,
     CHECK (PhotoID > 0),
     CHECK (DiskSizeNeeded >= 0),
     PRIMARY KEY (PhotoID));
     """
    create_disks_table = """
    CREATE TABLE Disks
    (DiskID INTEGER,
     ManufacturingCompany TEXT NOT NULL,
     Speed INTEGER NOT NULL,
     FreeSpace INTEGER NOT NULL,
     CostPerByte INTEGER NOT NULL,
     PRIMARY KEY (DiskID),
     CHECK (DiskID > 0),
     CHECK (Speed > 0),
     CHECK (CostPerByte > 0),
     CHECK(FreeSpace >= 0));
     """
    create_rams_table = """
     CREATE TABLE RAMs
    (RAMID INTEGER,
     Size INTEGER NOT NULL,
     Company TEXT NOT NULL,
     PRIMARY KEY (RAMID),
     CHECK (RAMID > 0),
     CHECK (Size > 0));
     """
    create_photos_on_disk_table = """
    CREATE TABLE PhotosOnDisk 
    (DiskID INTEGER,
     PhotoID INTEGER,
     PRIMARY KEY (DiskID, PhotoID),
     FOREIGN KEY (PhotoID)  REFERENCES Photos ON DELETE CASCADE,
     FOREIGN KEY (DiskID) REFERENCES disks ON DELETE CASCADE);
    """
    create_RAM_on_disk_table = """
    CREATE TABLE RAMsOnDisk
    (DiskID INTEGER,
    RAMID Integer,
    PRIMARY KEY (DiskID, RAMID),
    FOREIGN KEY (DiskID) REFERENCES Disks ON DELETE CASCADE,
    FOREIGN KEY (RAMID) References RAMs ON DELETE CASCADE);
    """

    create_pods_size_view = """
    CREATE VIEW PhotosOnDiskSize AS
     SELECT pod.PhotoID, pod.DiskID, p.DiskSizeNeeded
    FROM PhotosOnDisk pod INNER JOIN Photos p
        ON (p.PhotoID = pod.PhotoID);
    """

    create_rods_size_view = """
    CREATE VIEW RAMOnDiskSize AS
        SELECT rods.RAMID, rods.DiskID, r.Size
        FROM RAMsOnDisk rods INNER JOIN RAMs r
        ON (r.RAMID = rods.RAMID);
    """

    executeQuery(create_photos_table)
    executeQuery(create_disks_table)
    executeQuery(create_rams_table)
    executeQuery(create_photos_on_disk_table)
    executeQuery(create_RAM_on_disk_table)
    executeQuery(create_pods_size_view)
    executeQuery(create_rods_size_view)


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
    drop_pod_size_view = "DROP VIEW IF EXISTS PhotosOnDiskSize"
    executeQuery(drop_photos)
    executeQuery(drop_disks)
    executeQuery(drop_RAM)
    executeQuery(drop_photos_on_disk)
    executeQuery(drop_RAMs_on_disk)
    executeQuery(drop_pod_size_view)


def addPhoto(photo: Photo) -> ReturnValue:
    photo_query = sql.SQL("INSERT INTO Photos VALUES({ID}, {Description}, {Size})")\
        .format(ID=sql.Literal(photo.getPhotoID()),Description=sql.Literal(photo.getDescription()),
                Size=sql.Literal(photo.getSize()))
    return executeQuery(photo_query)


def getPhotoByID(photoID: int) -> Photo:
    conn = None
    rows_effected = 0
    try:
        conn = Connector.DBConnector()
        rows_effected, result = conn.execute(f"SELECT * FROM Photos WHERE PhotoID = {photoID}")
    except Exception as e:
        return Photo.badPhoto()

    finally:
        conn.close()
        return CreatePhotoFromResultSet(result, rows_effected)


def deletePhoto(photo: Photo) -> ReturnValue:
    del_photo_query = f"""
        BEGIN TRANSACTION;

        UPDATE Disks
        SET FreeSpace = FreeSpace + {photo.getSize()}
        WHERE DiskID = (SELECT DiskID FROM PhotosOnDisk
                        WHERE PhotoID = {photo.getPhotoID()});  
               
        DELETE FROM Photos 
        WHERE PhotoID = {photo.getPhotoID()};

        COMMIT;
        """

    return executeQuery(del_photo_query)


def addDisk(disk: Disk) -> ReturnValue:
    disk_query = sql.SQL("INSERT INTO Disks VALUES({ID}, {Manu}, {Speed}, {Space}, {Cost})") \
        .format(ID=sql.Literal(disk.getDiskID()), Manu=sql.Literal(disk.getCompany()),
                Speed=sql.Literal(disk.getSpeed()), Space=sql.Literal(disk.getFreeSpace()),
                Cost=sql.Literal(disk.getCost()))
    return executeQuery(disk_query)


def getDiskByID(diskID: int) -> Disk:
    conn = None
    rows_effected = 0
    try:
        conn = Connector.DBConnector()
        rows_effected, result = conn.execute(f"SELECT * FROM Disks WHERE DiskID = {diskID}")
    except Exception as e:
        return Disk.badDisk()
    finally:
        conn.close()
        return Disk(result[0]["DiskID"], result[0]["Company"], result[0]["Speed"], result[0]["FreeSpace"], result[0]["Cost"])


def deleteDisk(diskID: int) -> ReturnValue:
    conn = None
    rows_effected = 0
    try:
        conn = Connector.DBConnector()
        rows_effected, _ = conn.execute(f"DELETE FROM Disks WHERE DiskID = {diskID}")
    except Exception as e:
        return ReturnValue.ERROR

    finally:
        conn.close()
        return ReturnValue.OK  # make sure that it's deleted from all tables


def addRAM(ram: RAM) -> ReturnValue:
    RAM_query = sql.SQL("INSERT INTO RAMs VALUES({RAMID}, {Size}, {Company})") \
        .format(RAMID=sql.Literal(ram.getRamID()), Size=sql.Literal(ram.getSize()),
                Company=sql.Literal(ram.getCompany()))
    return executeQuery(RAM_query)

def getRAMByID(ramID: int) -> RAM:
    conn = None
    rows_effected = 0
    try:
        conn = Connector.DBConnector()
        rows_effected, result = conn.execute(f"SELECT * FROM RAMs WHERE RAMID = {ramID}")
    except Exception as e:
        return RAM.badRAM()
    finally:
        conn.close()
        return RAM(result[0]["RAMID"], result[0]["Size"], result[0]["Company"])


def deleteRAM(ramID: int) -> ReturnValue:
    conn = None
    rows_effected = 0
    try:
        conn = Connector.DBConnector()
        rows_effected, _ = conn.execute(f"DELETE FROM RAMs WHERE RAMID = {ramID}")
        conn.commit()
    except Exception as e:
        return ReturnValue.ERROR

    finally:
        conn.close()
        return ReturnValue.OK  # make sure that it's deleted from all tables


def addDiskAndPhoto(disk: Disk, photo: Photo) -> ReturnValue:

    disk_and_photo_query = sql.SQL("""
    START TRANSACTION;
    
    INSERT INTO Photos VALUES({PhotoID}, {Description}, {Size});
    INSERT INTO Disks VALUES({DiskID}, {Manu}, {Speed}, {Space}, {Cost})
    
    COMMIT;
    """)\
        .format(PhotoID=sql.Literal(photo.getPhotoID()), Description=sql.Literal(photo.getDescription()),
                Size=sql.Literal(photo.getSize()), DiskID=sql.Literal(disk.getDiskID()), Manu=sql.Literal(disk.getCompany()),
                Speed=sql.Literal(disk.getSpeed()), Space=sql.Literal(disk.getFreeSpace()),
                Cost=sql.Literal(disk.getCost()))

    return executeQuery(disk_and_photo_query)


def addPhotoToDisk(photo: Photo, diskID: int) -> ReturnValue:
    add_photo_to_disk_query = f"""
    BEGIN TRANSACTION;

    INSERT INTO PhotosOnDisk (DiskID, PhotoID)
    SELECT {diskID}, {photo.getPhotoID()}
    WHERE EXISTS (SELECT * FROM Disks
        WHERE DiskID = {diskID} AND FreeSpace >= {photo.getSize()});  
        
    UPDATE Disks
        SET FreeSpace = FreeSpace - {photo.getSize()}
        WHERE DiskID = (SELECT DiskID FROM PhotosOnDisk
                        WHERE PhotoID = {photo.getPhotoID()} AND DiskID = {diskID});  
                        
    COMMIT;              
        """

    return executeQuery(add_photo_to_disk_query)


def removePhotoFromDisk(photo: Photo, diskID: int) -> ReturnValue:
    remove_photo_from_disk_query = f"""
    BEGIN TRANSACTION;
    UPDATE Disks
        SET FreeSpace = FreeSpace + {photo.getSize()}
        WHERE DiskID = (SELECT DiskID FROM PhotosOnDisk
                        WHERE PhotoID = {photo.getPhotoID()} AND DiskID = {diskID});
                                            
    DELETE FROM PhotosOnDisk 
        WHERE PhotoID = {photo.getPhotoID()} AND DiskID = {diskID};

    COMMIT;              
        """

    return executeQuery(remove_photo_from_disk_query)


def addRAMToDisk(ramID: int, diskID: int) -> ReturnValue:
    ram_to_disk_query = f"""
        INSERT INTO RAMsOnDisk 
        VALUES({diskID}, {ramID});  
        """
    return executeQuery(ram_to_disk_query)


def removeRAMFromDisk(ramID: int, diskID: int) -> ReturnValue:
    remove_query = f"""
    DELETE FROM RAMsOnDisk 
        WHERE RAMID = {ramID} AND DiskID = {diskID};"""
    return executeQuery(remove_query)

def averagePhotosSizeOnDisk(diskID: int) -> float:
    average_size_query = f"""

    SELECT AVG(DiskSizeNeeded) as average_size
        FROM PhotosOnDiskSize
        WHERE DiskID = {diskID};

    """
    conn = None
    average_size = 0.0
    try:
        conn = Connector.DBConnector()
        rows_effected, result = conn.execute(average_size_query)
        average_size = float(result[0]['average_size'])
    except Exception as e:
        return -1

    finally:
        conn.close()
        return average_size


#MICHAL
def getTotalRamOnDisk(diskID: int) -> int:
    get_ram_query = sql.SQL("""
    SELECT SUM(Size) as total_ram
    FROM RAMOnDiskSize
    WHERE DiskID = {ID}
    """).format(ID=sql.Literal(diskID))
    conn = None
    total_ram = 0
    try:
        conn = Connector.DBConnector()
        rows_effected, result = conn.execute(get_ram_query)
        total_ram = int(result[0]["total_ram"])
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
        conn.rollback()
        return 0
    except Exception as e:
        return -1

    finally:
        conn.close()
        return total_ram


def getCostForDescription(description: str) -> int:

    get_cost_query = sql.SQL("""
    SELECT SUM(d.CostPerByte * temp.DiskSizeNeeded) AS total_cost
    FROM Disks d
    INNER JOIN (
        SELECT DiskID, DiskSizeNeeded
        FROM PhotosOnDiskSize
        WHERE PhotoID IN (
            SELECT PhotoID
            FROM Photos
            WHERE Description = {description})
    ) temp ON d.DiskID = temp.DiskID;""").format(description=sql.Literal(description))
    conn = None
    total_cost = 0
    try:
        conn = Connector.DBConnector()
        rows_effected, result = conn.execute(get_cost_query)
        conn.commit()
        total_cost = int(result[0]["total_cost"])
    except Exception as e:
        return -1

    finally:
        conn.close()
        return total_cost


##NOFAR
def getPhotosCanBeAddedToDisk(diskID: int) -> List[int]:
    return []

##MICHAL
def getPhotosCanBeAddedToDiskAndRAM(diskID: int) -> List[int]:
    photos_added_query = sql.SQL("""
    SELECT PhotoID 
    FROM Photos p
    WHERE (
        p.DiskSizeNeeded <= (
        SELECT FreeSpace FROM Disks WHERE DiskID = {diskID}
        ) AND p.DiskSizeNeeded < (
        SELECT SUM(Size) FROM RAMonDiskSize WHERE DiskID = {diskID}
        )
    )
    ORDER BY PhotoID ASC
    LIMIT 5
    """).format(diskID=sql.Literal(diskID))
    conn = None
    result = []
    try:
        conn = Connector.DBConnector()
        rows, result = conn.execute(photos_added_query)
    except Exception as e:
        return []
    finally:
        # will happen any way after try termination or exception handling
        conn.close()
        return [a for (a,) in result.rows]


##NOFAR
def isCompanyExclusive(diskID: int) -> bool:
    return True

##MICHAL
def isDiskContainingAtLeastNumExists(description : str, num : int) -> bool:
    disk_exists = sql.SQL("""
    SELECT EXISTS (
        SELECT 1
        FROM PhotosOnDisk pod 
        INNER JOIN Photos p ON pod.PhotoID = p.PhotoID
        WHERE p.Description = {description}
        GROUP BY pod.DiskID
        HAVING COUNT(*) >= {num}
    )
    """).format(description=sql.Literal(description), num=sql.Literal(num))

    conn = None
    try:
        conn = Connector.DBConnector()
        rows, result = conn.execute(disk_exists)
    except Exception as e:
        return False
    finally:
        # will happen any way after try termination or exception handling
        conn.close()
        return result[0]['exists']




def getDisksContainingTheMostData() -> List[int]:
    most_data_query = """
    SELECT DiskID 
    FROM PhotosOnDiskSize
    GROUP BY DiskID
    ORDER BY SUM(DiskSizeNeeded) DESC, DiskID ASC
    LIMIT 5
    """

    conn = None
    try:
        conn = Connector.DBConnector()
        rows, result = conn.execute(most_data_query)
        conn.commit()
    except DatabaseException.ConnectionInvalid as e:
        print(e)
    finally:
        # will happen any way after try termination or exception handling
        conn.close()
        return [a for (a,) in result.rows]

##NOFAR
def getConflictingDisks() -> List[int]:
    return []

##MICHAL
def mostAvailableDisks() -> List[int]:
    #we'll take only the disks that are able to save at least one photo. We'll then order them
    # by the number of photos they can save, the speed and diskID.
    most_available_query = sql.SQL("""
    SELECT DiskID
    FROM Disks
    WHERE FreeSpace >= (
        SELECT MIN(DiskSizeNeeded)
        FROM Photos
        )
    ORDER BY (
        SELECT COUNT(*)
        FROM Photos
        WHERE DiskSizeNeeded <= Disks.FreeSpace
    ) DESC, Speed Desc, DiskID ASC
    LIMIT 5
    """)
    conn = None
    try:
        conn = Connector.DBConnector()
        rows, result = conn.execute(most_available_query)
        conn.commit()
    except Exception as e:
        print(e)
    finally:
        # will happen any way after try termination or exception handling
        conn.close()
        return [a for (a,) in result.rows]

##NOFAR
def getClosePhotos(photoID: int) -> List[int]:
    return []


# if __name__ == '__main__':
#     dropTables()
#     print("Hello")
#     print("0. Creating all tables")
#     createTables()
#     addPhoto(Photo(1, "Nofar", 50))
#     addPhoto(Photo(2, "Nofar", 60))
#     our_photo = getPhotoByID(1)
#     addDisk(Disk(1,"Michal", 50, 300, 10))
#     addDisk(Disk(2, "Michal2", 50, 300, 10))
#     addDisk(Disk(3, "Michal2", 50, 300, 10))
#     addDisk(Disk(4, "Michal2", 50, 300, 10))
#     addDisk(Disk(5, "Michal2", 50, 300, 10))
#     addDisk(Disk(6, "Michal2", 50, 300, 10))
#     addDisk(Disk(7, "Michal2", 50, 300, 10))
#     addPhotoToDisk(Photo(1, "Nofar", 50), 1)
#     addPhotoToDisk(Photo(2, "Nofar", 60), 1)
#     addPhotoToDisk(Photo(2, "Nofar", 60), 2)
#     addPhotoToDisk(our_photo ,3)
#     addPhotoToDisk(our_photo, 4)
#     addPhotoToDisk(our_photo, 5)
#     addPhotoToDisk(our_photo, 6)
#     disk_one_average = averagePhotosSizeOnDisk(1)
#     cost_for_nofar = getCostForDescription("Nofar")
#     most_data = getDisksContainingTheMostData()
#     removePhotoFromDisk(Photo(1, "Nofar", 50), 1)
#     ret = removePhotoFromDisk(Photo(1, "Nofar", 50), 1)
#     dropTables()
# 
#     createTables()
#     addDisk(Disk(1, "Michal", 50, 300, 10))
#     addDisk(Disk(2, "Michal2", 50, 300, 10))
#     addDisk(Disk(3, "Michal3", 50, 300, 10))
# 
#     #deletePhoto(Photo(1, "Nofar", 50))
#     dropTables()
#     Test.test_RAM_add_get_and_remove()

if __name__ == '__main__':
    dropTables()
    createTables()
    print("adding rams and disks:")
    addRAM(RAM(1, "Dell", 50))
    addRAM(RAM(2, "HP", 50))
    addDisk(Disk(1, "Michal", 50, 300, 10))
    addDisk(Disk(2, "Michal2", 50, 300, 10))
    print("Adding photos:")
    addPhoto(Photo(1, "Nofar", 50))
    photo_one = getPhotoByID(1)
    addPhoto(Photo(2, "Nofar", 40))
    photo_two = getPhotoByID(2)
    print("adding ram 1 to disk 1:")
    addRAMToDisk(1,1)
    print ("adding ram 2 to disk 1:")
    addRAMToDisk(2,1)
    print("Total RAM on disk 1: ")
    print(getTotalRamOnDisk(1))
    print(getTotalRamOnDisk(3))
    print("photos that can be added to disk 1 and ram:")
    print(getPhotosCanBeAddedToDiskAndRAM(1))
    print("photos that can be added to disk 2:")
    print(getPhotosCanBeAddedToDisk(2))
    print("photos that can be added to disk 2 and ram:")
    print(getPhotosCanBeAddedToDiskAndRAM(2))
    print("adding photo 1 to disk1:")
    addPhotoToDisk(photo_one,1)
    print("adding photo 2 to disk1:")
    addPhotoToDisk(photo_two, 1)
    print("adding photo 1 to disk2:")
    addPhotoToDisk(photo_one,2)
    print("Checking If there is a disk containing at least one photo with Nofar as des (TRUE):")
    print(isDiskContainingAtLeastNumExists("Nofar",1))
    print("Checking If there is a disk containing at least three photos with Nofar as des (FALSE):")

    print(isDiskContainingAtLeastNumExists("Nofar",3))
    print("Checking If there is a disk containing at least one photo with MICHAL as des (FALSE):")

    print(isDiskContainingAtLeastNumExists("Michal",1))
    print("removing RAM 1 from disk 1:")
    removeRAMFromDisk(1,1)

    first_ram = getRAMByID(1)
    print("deleting RAM1:")
    deleteRAM(1)

    print("Adding disks to check most available disks")
    addDisk(Disk(3, "Michal3", 10, 300, 10))
    addDisk(Disk(4, "Michal4", 20, 300, 10))
    addDisk(Disk(5, "Michal5", 30, 300, 10))
    addDisk(Disk(6, "Michal6", 40, 300, 10))

    removePhotoFromDisk(photo_one, 1)
    removePhotoFromDisk(photo_two, 1)
    removePhotoFromDisk(photo_one,2)

    print("Six Disks. All have 300 free space. 5 should be returned, [1,2,6,5,4]:")
    print(mostAvailableDisks())
    print("A new photo is added of size 299. We'll put it on disk 1 and 3. Now return [2,6,5,4]")
    addPhoto(Photo(3, "Nofar", 299))
    photo_three = getPhotoByID(3)
    addPhotoToDisk(photo_three,1)
    addPhotoToDisk(photo_three,3)
    print(mostAvailableDisks())
    print("Add a smaller disk 7 that can only hold 2 of 3 photos. Should be added to the end of the list")
    addDisk(Disk(7, "Michal 7", 60, 100,10))
    print(mostAvailableDisks())
    dropTables()
