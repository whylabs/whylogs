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
import { getData } from "../hooks/GetData"

const ModalComp = ({ data, setData, dataEdit, isOpen, onClose }) => {
  // TODO Fetch this from entity schema API
  const columnOptions = [
    {
      col_name: "col1",
      col_type: "integral",
    },
    {
      col_name: "col2",
      col_type: "fractional",
    },
  ]

  const columnTypesToConstraints = new Map(
    // TODO either fetch results eagerly or treat it with Promises
    Object.entries(
      getData("http://localhost:8000/types_to_constraints")
        .constraints_per_datatype
    )
  )

  // TODO Fetch this from API
  const constraintsToValues = new Map([
    ["smaller_than_number", ["number"]],
    ["greater_than_number", ["number"]],
    ["null_count", ["number"]],
    ["mean_between_range", ["upper", "lower"]],
  ])

  const [column, setColumn] = useState(dataEdit.column || undefined)
  const [constraint, setConstraint] = useState(dataEdit.constraint || "")
  const [cons_value, setConsValue] = useState(dataEdit.cons_value || {})

  const renderConstraints = () => {
    if (!column) return null
    const constraints = columnTypesToConstraints.get(column.col_type)
    return constraints.map((option) => <option value={option}>{option}</option>)
  }

  const handleConsValue = (target) => {
    setConsValue({ ...cons_value, [target.name]: target.value })
  }

  const renderConstraintValues = () => {
    if (!constraint) return null

    const constraint_value = constraintsToValues.get(constraint)

    return constraint_value?.map((input) => (
      <FormLabel>
        {input}
        <Input
          type="text"
          name={input}
          value={cons_value[input]}
          onChange={(e) => handleConsValue(e.target)}
        />
      </FormLabel>
    ))
  }

  const handleSave = () => {
    if (!column || !constraint || !cons_value) return null

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
                  value={column?.col_name}
                  onChange={(e) =>
                    setColumn(
                      columnOptions.find(
                        (option) => option.col_name == e.target.value
                      )
                    )
                  }
                >
                  {columnOptions.map((option) => (
                    <option value={option.col_name}>{option.col_name}</option>
                  ))}
                </Select>
              </Box>
              <Box>
                <FormLabel>Constraint</FormLabel>
                <Select
                  placeholder="Select a constraint"
                  value={constraint}
                  onChange={(e) => setConstraint(e.target.value)}
                >
                  {renderConstraints()}
                </Select>
              </Box>
              <Box>
                <FormLabel>Values</FormLabel>
                {renderConstraintValues()}
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
