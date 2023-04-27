import {
  Modal,
  ModalOverlay,
  ModalContent,
  ModalHeader,
  ModalFooter,
  ModalBody,
  ModalCloseButton,
  Button,
  FormControl,
  FormLabel,
  Input,
  Box,
  Select,
} from "@chakra-ui/react"
import { useState } from "react"

const ModalComp = ({ data, setData, dataEdit, isOpen, onClose }) => {
  const columnOptions = [
    {
      col_name: "col1",
      col_type: "float",
    },
    {
      col_name: "col2",
      col_type: "int",
    },
  ]

  const columnTypesToConstraints = {
    str: ["frequent_items_n_max"],
    float: ["smaller_than_number", "greater_than_number", "mean_between_range"],
    int: ["null_count", "null_count_ratio"],
  }

  const [column, setColumn] = useState(dataEdit.column || "")
  const [constraint, setConstraint] = useState(dataEdit.constraint || "")
  const [cons_value, setConsValue] = useState(dataEdit.cons_value || "")

  const handleSave = () => {
    if (!column || !constraint || !cons_value) return

    if (Object.keys(dataEdit).length) {
      data[dataEdit.index] = { column, constraint, cons_value }
    }

    const newDataArray = !Object.keys(dataEdit).length
      ? [...(data ? data : []), { column, constraint, cons_value }]
      : [...(data ? data : [])]

    localStorage.setItem("constraint_set", JSON.stringify(newDataArray))

    setData(newDataArray)

    onClose()
  }

  return (
    <>
      <Modal isOpen={isOpen} onClose={onClose}>
        <ModalOverlay />
        <ModalContent>
          <ModalHeader>Add a constraint</ModalHeader>
          <ModalCloseButton />
          <ModalBody>
            <FormControl display="flex" flexDir="column" gap={4}>
              <Box>
                <FormLabel>Column</FormLabel>
                <Select
                  placeholder="Select a column"
                  value={column}
                  onChange={(e) => setColumn(e.target.value)}
                >
                  {columnOptions.map((option) => (
                    <option value={option.col_name}>{option.col_name}</option>
                  ))}
                  ;
                </Select>
              </Box>
              <Box>
                <FormLabel>Constraint</FormLabel>
                <Select
                  placeholder="Select a constraint"
                  value={constraint}
                  onChange={(e) => setConstraint(e.target.value)}
                >
                  {/* {columnTypesToConstraints[column].map((option) => (
                            <option value={option}>{option}</option>
                        ))}; */}
                </Select>
              </Box>
              <Box>
                <FormLabel>Values</FormLabel>
                <Input
                  type="text"
                  value={cons_value}
                  onChange={(e) => setConsValue(e.target.value)}
                />
              </Box>
            </FormControl>
          </ModalBody>

          <ModalFooter justifyContent="start">
            <Button colorScheme="green" mr={3} onClick={handleSave}>
              SAVE
            </Button>
            <Button colorScheme="gray" onClick={onClose}>
              CANCEL
            </Button>
          </ModalFooter>
        </ModalContent>
      </Modal>
    </>
  )
}

export default ModalComp
