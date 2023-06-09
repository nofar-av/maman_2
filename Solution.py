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


def CreateDiskFromResultSet(result_set: ResultSet, rows_affected: int) -> Disk:
    new_disk = Disk.badDisk()
    if (result_set is not None) and (rows_affected > 0):
        new_disk.setDiskID(result_set.rows[0][0])
        new_disk.setCompany(result_set.rows[0][1])
        new_disk.setSpeed(result_set.rows[0][2])
        new_disk.setFreeSpace(result_set.rows[0][3])
        new_disk.setCost(result_set.rows[0][4])
    return new_disk


def CreateRAMFromResultSet(result_set: ResultSet, rows_affected: int) -> RAM:
    new_ram = RAM.badRAM()
    if (result_set is not None) and (rows_affected > 0):
        new_ram.setRamID(result_set.rows[0][0])
        new_ram.setCompany(result_set.rows[0][1])
        new_ram.setSize(result_set.rows[0][2])

    return new_ram



def executeDelQuery(query) -> ReturnValue:
    conn = None
    res = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        rows_affected, result = conn.execute(query)
        conn.commit()
        if rows_affected == 0:
            res = ReturnValue.NOT_EXISTS
    except DatabaseException.ConnectionInvalid as e:
        
        conn.rollback()
        res = ReturnValue.ERROR
    except DatabaseException.UNIQUE_VIOLATION as e:
        
        conn.rollback()
        res = ReturnValue.ALREADY_EXISTS
    except DatabaseException.NOT_NULL_VIOLATION as e:
        
        conn.rollback()
        res = ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION as e:
        
        conn.rollback()
        res= ReturnValue.BAD_PARAMS

    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        
        conn.rollback()
        res = ReturnValue.NOT_EXISTS
    except Exception as e:
        
        conn.rollback()
        res = ReturnValue.ERROR
    finally:
        # will happen any way after try termination or exception handling
        conn.close()
        return res

def executeQuery(query) -> ReturnValue:
    conn = None
    res = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        conn.execute(query)
        conn.commit()
    except DatabaseException.ConnectionInvalid as e:
        
        conn.rollback()
        res = ReturnValue.ERROR
    except DatabaseException.UNIQUE_VIOLATION as e:
        
        conn.rollback()
        res = ReturnValue.ALREADY_EXISTS
    except DatabaseException.NOT_NULL_VIOLATION as e:
        
        conn.rollback()
        res = ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION as e:
        
        conn.rollback()
        res= ReturnValue.BAD_PARAMS

    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        
        conn.rollback()
        res = ReturnValue.NOT_EXISTS
    except Exception as e:
        conn.rollback()
        res = ReturnValue.ERROR
    finally:
        # will happen any way after try termination or exception handling
        conn.close()
        return res
    

def executeQueryBasic(query) -> ReturnValue:
    conn = None
    res = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        conn.execute(query)
        conn.commit()
    except Exception as e:
        conn.rollback()
        res = ReturnValue.ERROR
    finally:
        # will happen any way after try termination or exception handling
        conn.close()
        return res

def createTables()-> None:
    create_photos_table = """
    CREATE TABLE Photos 
    (PhotoID INTEGER,
     Description TEXT NOT NULL,
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
     Company TEXT NOT NULL,
     Size INTEGER NOT NULL,
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
    del_photo_query = sql.SQL("""
        BEGIN TRANSACTION;

        UPDATE Disks
        SET FreeSpace = FreeSpace + {size}
        WHERE DiskID IN (SELECT DiskID FROM PhotosOnDisk
                        WHERE PhotoID = {id});  
               
        DELETE FROM Photos 
        WHERE PhotoID = {id};

        COMMIT;
        """).format(size=sql.Literal(photo.getSize()), id=sql.Literal(photo.getPhotoID()))

    conn = None
    res = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        conn.execute(del_photo_query)
        conn.commit()
    except DatabaseException.ConnectionInvalid as e:
        conn.rollback()
        res = ReturnValue.ERROR
    except Exception as e:
        conn.rollback()
        res = ReturnValue.ERROR
    finally:
        # will happen any way after try termination or exception handling
        conn.close()
        return res


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
    return CreateDiskFromResultSet(result, rows_effected)

def deleteDisk(diskID: int) -> ReturnValue:
    query = sql.SQL("DELETE FROM Disks WHERE DiskID = {diskID}").format(diskID=sql.Literal(diskID))
    return executeDelQuery(query)  # make sure that it's deleted from all tables


def addRAM(ram: RAM) -> ReturnValue:
    RAM_query = sql.SQL("INSERT INTO RAMs VALUES({RAMID}, {Company}, {Size})") \
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
    return CreateRAMFromResultSet(result, rows_effected)

def deleteRAM(ramID: int) -> ReturnValue:
    query = sql.SQL("DELETE FROM RAMs WHERE RAMID = {ramID}").format(ramID=sql.Literal(ramID))
    return executeDelQuery(query)  # make sure that it's deleted from all tables


def addDiskAndPhoto(disk: Disk, photo: Photo) -> ReturnValue:

    disk_and_photo_query = sql.SQL("""
    START TRANSACTION;
    
    INSERT INTO Photos VALUES({PhotoID}, {Description}, {Size});
    INSERT INTO Disks VALUES({DiskID}, {Manu}, {Speed}, {Space}, {Cost});
    
    COMMIT;
    """)\
        .format(PhotoID=sql.Literal(photo.getPhotoID()), Description=sql.Literal(photo.getDescription()),
                Size=sql.Literal(photo.getSize()), DiskID=sql.Literal(disk.getDiskID()), Manu=sql.Literal(disk.getCompany()),
                Speed=sql.Literal(disk.getSpeed()), Space=sql.Literal(disk.getFreeSpace()),
                Cost=sql.Literal(disk.getCost()))
    conn = None
    rows_affected = 0
    ret = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        rows_affected, result = conn.execute(disk_and_photo_query)
    except DatabaseException.ConnectionInvalid as e:
        conn.rollback()
        ret = ReturnValue.ERROR
    except DatabaseException.UNIQUE_VIOLATION as e:
        conn.rollback()
        ret = ReturnValue.ALREADY_EXISTS
    except Exception as e:
        conn.rollback()
        ret = ReturnValue.ERROR
    finally:
        # will happen any way after try termination or exception handling
        conn.commit()
        conn.close()
    return ret

    return executeQuery(disk_and_photo_query)


def addPhotoToDisk(photo: Photo, diskID: int) -> ReturnValue:
    add_photo_to_disk_query = sql.SQL("""
    BEGIN TRANSACTION;
    
    INSERT INTO PhotosOnDisk VALUES( {diskID}, {photoID});
    
    UPDATE Disks
    SET FreeSpace = FreeSpace - {size}
    WHERE DiskID = {diskID};
            
    COMMIT;             
        """).format(photoID=sql.Literal(photo.getPhotoID()), diskID=sql.Literal(diskID), size=sql.Literal(photo.getSize()))
    conn = None
    rows_affected = 0
    ret = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        rows_affected, result = conn.execute(add_photo_to_disk_query)
        # if result and int(result[0]['updated']) == 0:
        #     ret = ReturnValue.BAD_PARAMS
    except DatabaseException.ConnectionInvalid as e:
        conn.rollback()
        ret = ReturnValue.ERROR
    except DatabaseException.CHECK_VIOLATION as e:
        conn.rollback()
        ret = ReturnValue.BAD_PARAMS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        conn.rollback()
        ret = ReturnValue.NOT_EXISTS
    except DatabaseException.UNIQUE_VIOLATION as e:
        conn.rollback()
        ret = ReturnValue.ALREADY_EXISTS
    except DatabaseException.NOT_NULL_VIOLATION as e:
        conn.rollback()
        ret = ReturnValue.NOT_EXISTS
    except Exception as e:
        conn.rollback()
        ret = ReturnValue.ERROR
    finally:
        # will happen any way after try termination or exception handling
        conn.commit()
        conn.close()
    return ret



def removePhotoFromDisk(photo: Photo, diskID: int) -> ReturnValue:
    remove_photo_from_disk_query = sql.SQL("""
    BEGIN TRANSACTION;
    UPDATE Disks
        SET FreeSpace = FreeSpace + {photoSize}
        WHERE DiskID = (SELECT DiskID FROM PhotosOnDisk
                        WHERE PhotoID = {photoID} AND DiskID = {diskID}) 
                        AND EXISTS(SELECT * FROM Photos
                        WHERE PhotoID = {photoID} AND Description = {description}
                        AND DiskSizeNeeded = {photoSize});
                                            
    DELETE FROM PhotosOnDisk 
        WHERE PhotoID = {photoID} AND DiskID = {diskID};

    COMMIT;              
        """).format(photoID=sql.Literal(photo.getPhotoID()), photoSize=sql.Literal(photo.getSize()),
                    diskID=sql.Literal(diskID),description=sql.Literal(photo.getDescription()))

    return executeQueryBasic(remove_photo_from_disk_query)


def addRAMToDisk(ramID: int, diskID: int) -> ReturnValue:
    ram_to_disk_query = f"""
        INSERT INTO RAMsOnDisk 
        VALUES({diskID}, {ramID});  
        """
    conn = None
    res = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        conn.execute(ram_to_disk_query)
        conn.commit()
    except DatabaseException.ConnectionInvalid as e:
        
        res = ReturnValue.ERROR
    except DatabaseException.UNIQUE_VIOLATION as e:
        
        res = ReturnValue.ALREADY_EXISTS
    except DatabaseException.NOT_NULL_VIOLATION as e:
        
        res = ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION as e:
        
        res= ReturnValue.ALREADY_EXISTS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        
        res = ReturnValue.NOT_EXISTS
    except Exception as e:
        res = ReturnValue.ERROR
    finally:
        # will happen any way after try termination or exception handling
        conn.close()
        return res


def removeRAMFromDisk(ramID: int, diskID: int) -> ReturnValue:
    remove_query = f"""
    DELETE FROM RAMsOnDisk 
        WHERE RAMID = {ramID} AND DiskID = {diskID};"""
    return executeDelQuery(remove_query)

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
        if result[0]['average_size']:
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
        if result and "total_ram" in result[0] and result[0]["total_ram"]:
            total_ram = int(result[0]["total_ram"])
        else:
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
        if result[0]["total_cost"]:
            total_cost = int(result[0]["total_cost"])
    except Exception as e:
        return -1

    finally:
        conn.close()
    return total_cost


##NOFAR
def getPhotosCanBeAddedToDisk(diskID: int) -> List[int]:
    photos_added_query = sql.SQL("""
    SELECT PhotoID 
    FROM Photos p
    WHERE p.DiskSizeNeeded <= 
        (SELECT FreeSpace FROM Disks WHERE DiskID = {diskID})
    ORDER BY PhotoID DESC
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
        conn.close()
    return [a for (a,) in result.rows]

##MICHAL
def getPhotosCanBeAddedToDiskAndRAM(diskID: int) -> List[int]:
    photos_added_query = sql.SQL("""
    SELECT PhotoID 
    FROM Photos p
    WHERE 
        (p.DiskSizeNeeded <= (
        SELECT FreeSpace FROM Disks 
		WHERE DiskID = {diskID}
        )
		AND ((p.DiskSizeNeeded <= (
        SELECT SUM(Size) FROM RAMonDiskSize WHERE DiskID = {diskID}
        ))
        OR (p.DiskSizeNeeded = 0 AND 
		 (SELECT COUNT(RAMID)
		 FROM RAMsOnDisk
		 WHERE DiskID = {diskID}) = 0
		)
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
    is_exclusive_query = sql.SQL("""
    SELECT COUNT(*)
    FROM (
		SELECT ManufacturingCompany
		FROM Disks
		WHERE DiskID = {diskID}) d
	   FULL OUTER JOIN
        (
        SELECT DISTINCT Company
        FROM RAMs 
        WHERE RAMID IN
        (SELECT RAMID
        FROM RAMsOnDisk
        WHERE DiskID = {diskID}        
        )
        ) AS RAMComp
    ON d.ManufacturingCompany = RAMComp.Company
    """).format(diskID=sql.Literal(diskID))
    conn = None
    result = []
    try:
        conn = Connector.DBConnector()
        rows, result = conn.execute(is_exclusive_query)
    except Exception as e:
        return False
    finally:
        # will happen any way after try termination or exception handling
        conn.close()
    result = [a for (a,) in result.rows]
    return result[0] == 1

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
    FROM Disks d
    GROUP BY DiskID
    ORDER BY 
    (SELECT COALESCE(SUM(DiskSizeNeeded),0)
    FROM PhotosOnDiskSize
    WHERE DiskID=d.DiskID) DESC, DiskID ASC
    LIMIT 5
    """

    conn = None
    try:
        conn = Connector.DBConnector()
        rows, result = conn.execute(most_data_query)
        conn.commit()
    except DatabaseException.ConnectionInvalid as e:
        return []
    finally:
        # will happen any way after try termination or exception handling
        conn.close()
    return [a for (a,) in result.rows]

##NOFAR
def getConflictingDisks() -> List[int]:
    conflicting_query = """SELECT DISTINCT DiskID
    From PhotosOnDisk
    Where PhotoID IN
    (SELECT DISTINCT PhotoID
    FROM PhotosOnDisk
    GROUP BY PhotoID
    HAVING COUNT(*) > 1)
    ORDER BY DiskID ASC"""

    conn = None
    try:
        conn = Connector.DBConnector()
        rows, result = conn.execute(conflicting_query)
        conn.commit()
    except DatabaseException.ConnectionInvalid as e:
        return []
    except Exception as e:
        return []
    finally:
        # will happen any way after try termination or exception handling
        conn.close()
    return [a for (a,) in result.rows]

##MICHAL
def mostAvailableDisks() -> List[int]:
    #we'll take only the disks that are able to save at least one photo. We'll then order them
    # by the number of photos they can save, the speed and diskID.
    most_available_query = sql.SQL("""
    SELECT DiskID
    FROM Disks
    
    ORDER BY (
        SELECT COUNT(*)
        FROM Photos
        WHERE DiskSizeNeeded <= Disks.FreeSpace
    ) DESC, Speed Desc, DiskID ASC
    LIMIT 5
    """)
    #WHERE FreeSpace >= (SELECT MIN(DiskSizeNeeded) FROM Photos)
    conn = None
    try:
        conn = Connector.DBConnector()
        rows, result = conn.execute(most_available_query)
        conn.commit()
    except Exception as e:
        return []
    finally:
        # will happen any way after try termination or exception handling
        conn.close()
    return [a for (a,) in result.rows]

##NOFAR
def getClosePhotos(photoID: int) -> List[int]:

    close_query =sql.SQL("""
    SELECT PhotoID
    FROM Photos p
    WHERE PhotoID <> {ID} AND (
    
    (SELECT COUNT(DiskID)
    FROM PhotosOnDisk
    Where PhotoID =p.PhotoID AND DiskID IN 
    (SELECT DiskID
    FROM PhotosOnDisk
    WHERE PhotoID = {ID})) >=
    
    (SELECT COUNT(DiskID) * 0.5
    FROM PhotosOnDisk
    WHERE PhotoID ={ID})
        )
    ORDER BY p.PhotoID ASC
    LIMIT 10""").format(ID=sql.Literal(photoID))
    
    conn = None
    try:
        conn = Connector.DBConnector()
        rows, result = conn.execute(close_query)
        conn.commit()
    except DatabaseException.ConnectionInvalid as e:
        return []
    except Exception as e:
        return []
    finally:
        # will happen any way after try termination or exception handling
        conn.close()
    return [a for (a,) in result.rows]


