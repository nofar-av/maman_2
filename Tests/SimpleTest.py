import unittest
import Solution
from Utility.ReturnValue import ReturnValue
from Tests.abstractTest import AbstractTest
from Business.Photo import Photo
from Business.RAM import RAM
from Business.Disk import Disk

'''
    Simple test, create one of your own
    make sure the tests' names start with test_
'''


class Test(AbstractTest):
    def test_Disk(self) -> None:
        self.assertEqual(ReturnValue.OK, Solution.addDisk(Disk(1, "DELL", 10, 10, 10)), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addDisk(Disk(2, "DELL", 10, 10, 10)), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addDisk(Disk(3, "DELL", 10, 10, 10)), "Should work")
        self.assertEqual(ReturnValue.ALREADY_EXISTS, Solution.addDisk(Disk(1, "DELL", 10, 10, 10)),
                         "ID 1 already exists")

    def test_RAM(self) -> None:
        self.assertEqual(ReturnValue.OK, Solution.addRAM(RAM(1, "find minimum value", 10)), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addRAM(RAM(2, "find minimum value", 10)), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addRAM(RAM(3, "find minimum value", 10)), "Should work")
        self.assertEqual(ReturnValue.ALREADY_EXISTS, Solution.addRAM(RAM(3, "find minimum value", 10)),
                         "ID 1 already exists")

    def test_Photo(self) -> None:
        self.assertEqual(ReturnValue.OK, Solution.addPhoto(Photo(1, "Tree", 10)), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addPhoto(Photo(2, "Tree", 10)), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addPhoto(Photo(3, "Tree", 10)), "Should work")
        self.assertEqual(ReturnValue.ALREADY_EXISTS, Solution.addPhoto(Photo(3, "Tree", 10)),
                         "ID 1 already exists")
    def test_isDiskContainingAtLeastNumExists(self) -> None:
        Solution.clearTables()
        self.assertEqual(False, Solution.isDiskContainingAtLeastNumExists("hello",1), "no photos")
        self.assertEqual(ReturnValue.OK, Solution.addPhoto(Photo(1,"hello",1)), "Should work")
        self.assertEqual(False, Solution.isDiskContainingAtLeastNumExists("hello",1), "no disks")
        self.assertEqual(ReturnValue.OK, Solution.addDisk(Disk(1, "b",1,10, 1)), "Should work")
        self.assertEqual(False, Solution.isDiskContainingAtLeastNumExists("hellooooo",1), "no pjotos with description")
        self.assertEqual(False, Solution.isDiskContainingAtLeastNumExists("hello",1), "no photos on disks")
        self.assertEqual(ReturnValue.OK, Solution.addPhotoToDisk(Photo(1,"hello",1), 1), "Should work")
        self.assertEqual(True, Solution.isDiskContainingAtLeastNumExists("hello",1), "good")
        self.assertEqual(ReturnValue.OK, Solution.addDisk(Disk(2, "a",1,5, 1)), "Should work")
        self.assertEqual(ReturnValue.ALREADY_EXISTS, Solution.addPhotoToDisk(Photo(1,"hello",1), 1), "Shouldn't work")
        self.assertEqual(ReturnValue.ALREADY_EXISTS, Solution.addPhotoToDisk(Photo(1,"hello",1), 2), "Shouldn't work")
        self.assertEqual(False, Solution.isDiskContainingAtLeastNumExists("hello",2), "only one")

        self.assertEqual(ReturnValue.OK, Solution.addPhoto(Photo(2,"hello",1)), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addPhoto(Photo(3,"HELLO",1)), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addPhotoToDisk(Photo(3,"HELLO",1), 1), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addPhoto(Photo(4,"llll",1)), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addPhotoToDisk(Photo(4,"llll",1), 1), "Should work")
        self.assertEqual(False, Solution.isDiskContainingAtLeastNumExists("hello",2), "only one")
        self.assertEqual(ReturnValue.OK, Solution.addPhotoToDisk(Photo(2,"hello",1), 1), "Should work")
        self.assertEqual(True, Solution.isDiskContainingAtLeastNumExists("hello",2), "disk 2 has 2 photos")

    def test_getDisksContainingTheMostData(self) -> None:
        Solution.clearTables()
        self.assertEqual(ReturnValue.OK, Solution.addPhoto(Photo(1,"hello",1)), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addPhoto(Photo(2,"hello",2)), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addPhoto(Photo(3,"hello",2)), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addPhoto(Photo(4,"hello",3)), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addPhoto(Photo(5,"hello",4)), "Should work")

        self.assertEqual(ReturnValue.OK, Solution.addDisk(Disk(1, "b",1,100, 1)), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addDisk(Disk(2, "b",1,100, 1)), "Should work")
        self.assertEqual([1,2], Solution.getDisksContainingTheMostData(), "only disks 1,2 exist")
        self.assertEqual(ReturnValue.OK, Solution.addDisk(Disk(3, "b",1,100, 1)), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addDisk(Disk(4, "b",1,100, 1)), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addDisk(Disk(5, "b",1,100, 1)), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addDisk(Disk(6, "b",1,100, 1)), "Should work")
        self.assertEqual([1,2,3,4,5], Solution.getDisksContainingTheMostData(), "all disks empty")

        self.assertEqual(ReturnValue.OK, Solution.addPhotoToDisk(Photo(1,"hello",1), 2), "Should work")
        self.assertEqual([2,1,3,4,5], Solution.getDisksContainingTheMostData(), "only disk 2 has photos")
        self.assertEqual(ReturnValue.OK, Solution.addPhotoToDisk(Photo(2,"hello",2), 2), "Should work")
        self.assertEqual([2,1,3,4,5], Solution.getDisksContainingTheMostData(), "only disk 2 has photos")
        self.assertEqual(ReturnValue.OK, Solution.addPhotoToDisk(Photo(2,"hello",2), 6), "Should work")
        self.assertEqual([2,6,1,3,4], Solution.getDisksContainingTheMostData(), "2 -3 , 6- 2")
        self.assertEqual(ReturnValue.OK, Solution.addPhotoToDisk(Photo(3,"hello",2), 5), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addPhotoToDisk(Photo(3,"hello",2), 6), "Should work")
        self.assertEqual([6,2,5,1,3], Solution.getDisksContainingTheMostData(), "2 -3 , 6- 4, 5- 2")
        
        self.assertEqual(ReturnValue.OK, Solution.addPhotoToDisk(Photo(1,"hello",1), 3), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addPhotoToDisk(Photo(2,"hello",2), 3), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addPhotoToDisk(Photo(3,"hello",2), 3), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addPhotoToDisk(Photo(4,"hello",3), 3), "Should work")
        self.assertEqual([3,6,2,5,1], Solution.getDisksContainingTheMostData(), "3- 8, 2 -3 , 6- 4, 5- 2")
        self.assertEqual(ReturnValue.OK, Solution.addPhotoToDisk(Photo(4,"hello",3), 2), "Should work")
        self.assertEqual([3,2,6,5,1], Solution.getDisksContainingTheMostData(), "3- 8, 2 -6 , 6- 4, 5- 2")
        self.assertEqual(ReturnValue.OK, Solution.addPhotoToDisk(Photo(2,"hello",2), 1), "Should work")
        self.assertEqual([3,2,6,1,5], Solution.getDisksContainingTheMostData(), "3- 8, 2 -6 , 6- 4, 5- 2, 1-2")




                         



if __name__ == '__main__':
    unittest.main(verbosity=2, exit=False)
