package main

import (
	"encoding/csv"
	"fmt"
	"github.com/golang/protobuf/proto"
	"io/ioutil"
	"log"
	"os"
	"sftp/pb"
	"strconv"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
)

func main() {
	// Generate sample Protobuf data
	data := generateSampleProtobuf(1000000)

	// Create a message to hold the array of ReportDataV4
	message := &pb.ReportsList{
		Reports: data,
	}

	// Marshal the message into a byte array
	serializedData, err := proto.Marshal(message)
	if err != nil {
		log.Fatalf("failed to marshal protobuf: %v", err)
	}

	// Measure time taken for local file write
	startTime := time.Now()
	err = ioutil.WriteFile("data.bin", serializedData, 0644)
	if err != nil {
		log.Fatalf("failed to write protobuf to local file: %v", err)
	}
	uploadDuration := time.Since(startTime)
	fmt.Printf("Protobuf data successfully serialized and uploaded to local file in %v\n", uploadDuration)

	// Read the serialized data from the file
	startTime = time.Now()
	readData, err := ioutil.ReadFile("data.bin")
	if err != nil {
		log.Fatalf("failed to read protobuf from local file: %v", err)
	}
	readDuration := time.Since(startTime)
	fmt.Printf("Protobuf data successfully read from local file in %v\n", readDuration)

	startTime = time.Now()
	// Create a new message to hold the deserialized data
	var readMessage pb.ReportsList

	// Unmarshal the data into the message
	err = proto.Unmarshal(readData, &readMessage)
	if err != nil {
		log.Fatalf("failed to unmarshal protobuf: %v", err)
	}

	fmt.Println("Protobuf data successfully unmarshaled")

	// Save deserialized data to a CSV file
	err = saveToCSV("data.csv", readMessage.Reports)
	if err != nil {
		log.Fatalf("failed to save to CSV: %v", err)
	}
	unMarshalledDuration := time.Since(startTime)
	fmt.Printf("Protobuf data successfully saved to CSV file: %v\n", unMarshalledDuration)
}

func generateSampleProtobuf(numEntries int) []*pb.ReportDataV4 {
	var data []*pb.ReportDataV4

	for i := 0; i < numEntries; i++ {
		entry := &pb.ReportDataV4{
			BookingStatus:          "confirmed",
			BookingDate:            timestamppb.New(time.Now()),
			StartTime:              timestamppb.New(time.Now().Add(time.Hour)),
			DeliveryMedium:         "Medium" + strconv.Itoa(i),
			Category:               "Category" + strconv.Itoa(i),
			Duration:               int32(i),
			ServiceType:            "Type" + strconv.Itoa(i),
			ServiceName:            "Name" + strconv.Itoa(i),
			BookingType:            "Type" + strconv.Itoa(i),
			ChannelName:            "Channel" + strconv.Itoa(i),
			StaffFirstName:         "First" + strconv.Itoa(i),
			StaffLastName:          "Last" + strconv.Itoa(i),
			StaffEmail:             "email" + strconv.Itoa(i) + "@example.com",
			StaffRole:              "Role" + strconv.Itoa(i),
			CustomerEmail:          "customer" + strconv.Itoa(i) + "@example.com",
			BuildingCode:           "Code" + strconv.Itoa(i),
			RoomName:               "Room" + strconv.Itoa(i),
			RoomCode:               "Code" + strconv.Itoa(i),
			BookingId:              "ID" + strconv.Itoa(i),
			ClassId:                "Class" + strconv.Itoa(i),
			BookedByEmail:          "booked" + strconv.Itoa(i) + "@example.com",
			UpdatedDateTime:        timestamppb.New(time.Now()),
			UpdatedBy:              "UpdatedBy" + strconv.Itoa(i),
			CostCategory:           "Category" + strconv.Itoa(i),
			CostTier:               "Tier" + strconv.Itoa(i),
			AmountPaid:             float32(i) * 0.5,
			AmountRefunded:         float32(i) * 0.3,
			ReminderSentYn:         "Y",
			ItemType:               "Item" + strconv.Itoa(i),
			CurrencyType:           "Currency",
			PaymentStatus:          "Status" + strconv.Itoa(i),
			PaymentTimestamp:       timestamppb.New(time.Now()),
			PaymentRefundDate:      timestamppb.New(time.Now()),
			PaymentReason:          "Reason" + strconv.Itoa(i),
			Timezone:               "Timezone" + strconv.Itoa(i),
			CapacityConfirmed:      int64(i),
			CapacityWaitlist:       int64(i),
			UserDepartment:         "Department" + strconv.Itoa(i),
			UserBusiness:           "Business" + strconv.Itoa(i),
			DivisionConpanyCode:    "DivisionCode" + strconv.Itoa(i),
			LocalStartTime:         "StartTime" + strconv.Itoa(i),
			DayOfWeek:              "Day" + strconv.Itoa(i),
			LocationCode:           "Code" + strconv.Itoa(i),
			ShiftId:                "Shift" + strconv.Itoa(i),
			ExternalAttendeesCount: int64(i),
		}
		data = append(data, entry)
	}

	return data
}

func saveToCSV(filename string, data []*pb.ReportDataV4) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write header
	header := []string{
		"BookingStatus", "BookingDate", "StartTime", "DeliveryMedium", "Category", "Duration", "ServiceType",
		"ServiceName", "BookingType", "ChannelName", "StaffFirstName", "StaffLastName", "StaffEmail", "StaffRole",
		"CustomerEmail", "BuildingCode", "RoomName", "RoomCode", "BookingId", "ClassId", "BookedByEmail",
		"UpdatedDateTime", "UpdatedBy", "CostCategory", "CostTier", "AmountPaid", "AmountRefunded",
		"ReminderSentYn", "ItemType", "CurrencyType", "PaymentStatus", "PaymentTimestamp", "PaymentRefundDate",
		"PaymentReason", "Timezone", "CapacityConfirmed", "CapacityWaitlist", "UserDepartment", "UserBusiness",
		"DivisionConpanyCode", "LocalStartTime", "DayOfWeek", "LocationCode", "ShiftId", "ExternalAttendeesCount",
	}
	err = writer.Write(header)
	if err != nil {
		return err
	}

	// Write data
	for _, entry := range data {
		record := []string{
			entry.BookingStatus,
			entry.BookingDate.AsTime().String(),
			entry.StartTime.AsTime().String(),
			entry.DeliveryMedium,
			entry.Category,
			strconv.Itoa(int(entry.Duration)),
			entry.ServiceType,
			entry.ServiceName,
			entry.BookingType,
			entry.ChannelName,
			entry.StaffFirstName,
			entry.StaffLastName,
			entry.StaffEmail,
			entry.StaffRole,
			entry.CustomerEmail,
			entry.BuildingCode,
			entry.RoomName,
			entry.RoomCode,
			entry.BookingId,
			entry.ClassId,
			entry.BookedByEmail,
			entry.UpdatedDateTime.AsTime().String(),
			entry.UpdatedBy,
			entry.CostCategory,
			entry.CostTier,
			fmt.Sprintf("%.2f", entry.AmountPaid),
			fmt.Sprintf("%.2f", entry.AmountRefunded),
			entry.ReminderSentYn,
			entry.ItemType,
			entry.CurrencyType,
			entry.PaymentStatus,
			entry.PaymentTimestamp.AsTime().String(),
			entry.PaymentRefundDate.AsTime().String(),
			entry.PaymentReason,
			entry.Timezone,
			strconv.Itoa(int(entry.CapacityConfirmed)),
			strconv.Itoa(int(entry.CapacityWaitlist)),
			entry.UserDepartment,
			entry.UserBusiness,
			entry.DivisionConpanyCode,
			entry.LocalStartTime,
			entry.DayOfWeek,
			entry.LocationCode,
			entry.ShiftId,
			strconv.Itoa(int(entry.ExternalAttendeesCount)),
		}
		err = writer.Write(record)
		if err != nil {
			return err
		}
	}

	return nil
}
