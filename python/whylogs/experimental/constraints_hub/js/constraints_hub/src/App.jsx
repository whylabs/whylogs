import { EditIcon, DeleteIcon } from "@chakra-ui/icons"
import {
  Box,
  Flex,
  Button,
  useDisclosure,
  Table,
  Thead,
  Tr,
  Th,
  Tbody,
  Td,
  useBreakpointValue,
} from "@chakra-ui/react"
import { useEffect, useState } from "react"
import ModalComp from "./components/ModalComp"
import { getData } from "./hooks/GetData"

const App = () => {
  const { isOpen, onOpen, onClose } = useDisclosure()
  const [data, setData] = useState([])
  const [dataEdit, setDataEdit] = useState({})

  useEffect(() => {
    const db_costumer = localStorage.getItem("constraint_set")
      ? JSON.parse(localStorage.getItem("constraint_set"))
      : []

    setData(db_costumer)
  }, [setData])

  const handleRemove = (column, constraint, cons_value) => {
    const newArray = data.filter(
      (item) =>
        item.column !== column &&
        item.constraint !== constraint &&
        item.cons_value !== cons_value
    )

    setData(newArray)

    localStorage.setItem("constraint_set", JSON.stringify(newArray))
  }

  const handlePush = () => {
    // console.log(getData("http://localhost:8000/types_to_constraints"))
    console.log(JSON.parse(localStorage.getItem("constraint_set")))
  }

  return (
    <Flex
      h="100vh"
      align="center"
      justify="center"
      fontSize="20px"
      fontFamily="poppins"
    >
      <Box maxW={800} w="100%" h="100vh" py={10} px={2}>
        <Box overflowY="auto" height="90%">
          <Table mt="6">
            <Thead>
              <Tr>
                <Th fontSize="20px">Column</Th>
                <Th fontSize="20px">Constraint</Th>
                <Th fontSize="20px">Values</Th>

                <Th p={0}></Th>
                <Th p={0}></Th>
              </Tr>
            </Thead>
            <Tbody>
              {data.map(({ column, constraint, cons_value }, index) => (
                <Tr key={index} cursor="pointer " _hover={{ bg: "gray.100" }}>
                  <Td>{column.col_name}</Td>
                  <Td>{constraint}</Td>
                  <Td>{JSON.stringify(cons_value)}</Td>
                  <Td p={0}>
                    <EditIcon
                      fontSize={20}
                      onClick={() => [
                        setDataEdit({ column, constraint, cons_value, index }),
                        onOpen(),
                      ]}
                    />
                  </Td>
                  <Td p={0}>
                    <DeleteIcon
                      fontSize={20}
                      onClick={() =>
                        handleRemove(column, constraint, cons_value)
                      }
                    />
                  </Td>
                </Tr>
              ))}
            </Tbody>
          </Table>
        </Box>
        <Button
          colorScheme="blue"
          onClick={() => [setDataEdit({}), onOpen()]}
          mx={2}
        >
          NEW CONSTRAINT
        </Button>

        <Button colorScheme="purple" onClick={handlePush()} mx={2}>
          PUSH
        </Button>
      </Box>
      {isOpen && (
        <ModalComp
          isOpen={isOpen}
          onClose={onClose}
          data={data}
          setData={setData}
          dataEdit={dataEdit}
          setDataEdit={setDataEdit}
        />
      )}
    </Flex>
  )
}

export default App
