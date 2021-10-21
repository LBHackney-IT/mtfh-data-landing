using Microsoft.Extensions.Logging;
using MTFHDataLanding.Boundary;
using MTFHDataLanding.Gateway.Interfaces;
using MTFHDataLanding.Infrastructure.Exceptions;
using MTFHDataLanding.UseCase.Interfaces;
using Hackney.Core.Logging;
using System;
using System.Threading.Tasks;
using Parquet;
using Parquet.Data;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Amazon;
using Amazon.S3;
using Amazon.S3.Transfer;

namespace MTFHDataLanding.UseCase
{
    public class LandTenureData : ILandTenureData
    {
        private readonly ITenureInfoApi _tenureApi;
        private readonly ILogger<LandTenureData> _logger;

        public LandTenureData(ITenureInfoApi tenureApi,
            ILogger<LandTenureData> logger)
        {
            _tenureApi = tenureApi;
            _logger = logger;
        }

        [LogCall]
        public async Task ProcessMessageAsync(EntityEventSns message)
        {
            String bucketName = "mtfh-data-landing-spike";
            String keyName = "landing/mtfh/tenures/";
            RegionEndpoint bucketRegion = RegionEndpoint.EUWest2;

            IAmazonS3 s3Client = new AmazonS3Client(bucketRegion);
            var fileTransferUtility = new TransferUtility(s3Client);

            if (message is null) throw new ArgumentNullException(nameof(message));

            // #1 - Get the tenure
            var tenure = await _tenureApi.GetTenureInfoByIdAsync(message.EntityId, message.CorrelationId)
                                             .ConfigureAwait(false);
            if (tenure is null) throw new TenureNotFoundException(message.EntityId);

            _logger.LogWarning($"Tenure record (id: {tenure.Id})");
            var intTenureHouseholdMembers = new int[tenure.HouseholdMembers.Count()];
            var householdMembersIdList = new string[tenure.HouseholdMembers.Count()];
            var householdMembersTypeList = new string[tenure.HouseholdMembers.Count()];
            var householdMembersFullNameList = new string[tenure.HouseholdMembers.Count()];
            var householdMembersIsResponsibleList = new bool[tenure.HouseholdMembers.Count()];
            var householdMembersDateOfBirthList = new string[tenure.HouseholdMembers.Count()];
            var householdMembersPersonTenureTypeList = new string[tenure.HouseholdMembers.Count()];
            int index = 0;
            foreach (var householdMember in tenure.HouseholdMembers)
            {
                if (index != 0)
                {
                    intTenureHouseholdMembers[index] = 1;
                }
                householdMembersIdList[index] = (householdMember.Id != null ? householdMember.Id.ToString() : "");
                householdMembersTypeList[index] = householdMember.Type.ToString();
                householdMembersFullNameList[index] = (householdMember.FullName != null ? householdMember.FullName.ToString() : "");
                householdMembersIsResponsibleList[index] = householdMember.IsResponsible;
                householdMembersDateOfBirthList[index] = (householdMember.DateOfBirth != null ? householdMember.DateOfBirth.ToString() : "");
                householdMembersPersonTenureTypeList[index] = householdMember.PersonTenureType.ToString();
                index++;
            }

            var intNotices = new int[tenure.Notices.Count()];
            var noticesTypeList = new string[tenure.Notices.Count()];
            var noticesServedDateList = new string[tenure.Notices.Count()];
            var noticesExpiryDateList = new string[tenure.Notices.Count()];
            var noticesEndDateList = new string[tenure.Notices.Count()];
            var noticesEffectiveDateList = new string[tenure.Notices.Count()];
            index = 0;
            foreach (var notice in tenure.Notices)
            {
                if (index != 0)
                {
                    intNotices[index] = 1;
                }
                noticesTypeList[index] = (notice.Type != null ? notice.Type.ToString() : "");
                noticesServedDateList[index] = (notice.ServedDate != null ? notice.ServedDate.ToString() : "");
                noticesExpiryDateList[index] = (notice.ExpiryDate != null ? notice.ExpiryDate.ToString() : "");
                noticesEndDateList[index] = (notice.EndDate != null ? notice.EndDate.ToString() : "");
                noticesEffectiveDateList[index] = (notice.EffectiveDate != null ? notice.EffectiveDate.ToString() : "");
                index++;
            }

            var intLegacyReferences = new int[tenure.LegacyReferences.Count()];
            var legacyReferencesNameList = new string[tenure.LegacyReferences.Count()];
            var legacyReferencesValueList = new string[tenure.LegacyReferences.Count()];
            index = 0;
            foreach (var legacyReference in tenure.LegacyReferences)
            {
                if (index != 0)
                {
                    intLegacyReferences[index] = 1;
                }
                legacyReferencesNameList[index] = (legacyReference.Name != null ? legacyReference.Name : "");
                legacyReferencesValueList[index] = (legacyReference.Value != null ? legacyReference.Value : "");
                index++;
            }

            var id = new DataColumn(new DataField<string>("id"), new string[] { tenure.Id.ToString() });
            var paymentReference = new DataColumn(new DataField<string>("paymentReference"), new string[] {
            tenure.PaymentReference != null ? tenure.PaymentReference : "" });

            var householdMembersId = new DataColumn(new DataField<string>("id"), householdMembersIdList, intTenureHouseholdMembers);
            var householdMembersType = new DataColumn(new DataField<string>("type"), householdMembersTypeList, intTenureHouseholdMembers);
            var householdMembersFullName = new DataColumn(new DataField<string>("fullName"), householdMembersFullNameList, intTenureHouseholdMembers);
            var householdMembersIsResponsible = new DataColumn(new DataField<bool>("isResponsible"), householdMembersIsResponsibleList, intTenureHouseholdMembers);
            var householdMembersDateOfBirth = new DataColumn(new DataField<string>("dateOfBirth"), householdMembersDateOfBirthList, intTenureHouseholdMembers);
            var householdMembersPersonTenureType = new DataColumn(new DataField<string>("personTenureType"), householdMembersPersonTenureTypeList, intTenureHouseholdMembers);

            var tenuredAssetId = new DataColumn(new DataField<string>("id"), new string[] { tenure.TenuredAsset.Id.ToString() });
            var tenuredAssetType = new DataColumn(new DataField<string>("type"), new string[] { tenure.TenuredAsset.Type.ToString() });
            var tenuredAssetFullAddress = new DataColumn(new DataField<string>("fullAddress"), new string[] { tenure.TenuredAsset.FullAddress });
            var tenuredAssetUprn = new DataColumn(new DataField<string>("uprn"), new string[] { tenure.TenuredAsset.Uprn });

            // var chargesRent = new DataColumn(new DataField<float>("rent"), new float[] { tenure.Charges.Rent });
            // var chargesCurrentBalance = new DataColumn(new DataField<float>("currentBalance"), new float[] { tenure.Charges.CurrentBalance });
            // var chargesBillingFrequency = new DataColumn(new DataField<string>("billingFrequency"), new string[] { tenure.Charges.BillingFrequency });
            // var chargesServiceCharge = new DataColumn(new DataField<float>("serviceCharge"), new float[] { tenure.Charges.ServiceCharge });
            // var chargesOtherCharges = new DataColumn(new DataField<float>("otherCharges"), new float[] { tenure.Charges.OtherCharges });
            // var chargesCombinedServiceCharges = new DataColumn(new DataField<float>("combinedServiceCharges"), new float[] { tenure.Charges.CombinedServiceCharges });
            // var chargesCombinedRentCharges = new DataColumn(new DataField<float>("combinedRentCharges"), new float[] { tenure.Charges.CombinedRentCharges });
            // var chargesTenancyInsuranceCharge = new DataColumn(new DataField<float>("tenancyInsuranceCharge"), new float[] { tenure.Charges.TenancyInsuranceCharge });
            // var chargesOriginalRentCharge = new DataColumn(new DataField<float>("originalRentCharge"), new float[] { tenure.Charges.OriginalRentCharge });
            // var chargesOriginalServiceCharge = new DataColumn(new DataField<float>("originalServiceCharge"), new float[] { tenure.Charges.OriginalServiceCharge });

            var startOfTenureDate = new DataColumn(new DataField<string>("startOfTenureDate"), new string[] { tenure.StartOfTenureDate.ToString() });
            var endOfTenureDate = new DataColumn(new DataField<string>("endOfTenureDate"), new string[] {
            tenure.EndOfTenureDate != null ? tenure.EndOfTenureDate.ToString() : "" });

            var tenureTypeCode = new DataColumn(new DataField<string>("code"), new string[] { tenure.TenureType.Code });
            var tenureTypeDescription = new DataColumn(new DataField<string>("description"), new string[] { tenure.TenureType.Description });

            // var isTenanted = new DataColumn(new DataField<bool>("isTenanted"), new bool[] { (bool) tenure.IsTenanted });

            // var terminatedIsTerminated = new DataColumn(new DataField<bool>("isTerminated"), new bool[] { tenure.Terminated.IsTerminated });
            // var terminatedReasonForTermination = new DataColumn(new DataField<string>("reasonForTermination"), new string[] { tenure.Terminated.ReasonForTermination });

            // var successionDate = new DataColumn(new DataField<string>("successionDate"), new string[] { tenure.SuccessionDate.ToString() });
            // var evictionDate = new DataColumn(new DataField<string>("evictionDate"), new string[] { tenure.EvictionDate.ToString() });
            // var potentialEndDate = new DataColumn(new DataField<string>("potentialEndDate"), new string[] { tenure.PotentialEndDate.ToString() });

            // var noticesType = new DataColumn(new DataField<string>("type"), noticesTypeList, intNotices);
            // var noticesServedDate = new DataColumn(new DataField<string>("servedDate"), noticesServedDateList, intNotices);
            // var noticesExpiryDate = new DataColumn(new DataField<string>("expiryDate"), noticesExpiryDateList, intNotices);
            // var noticesEndDate = new DataColumn(new DataField<string>("endDate"), noticesEndDateList, intNotices);
            // var noticesEffectiveDate = new DataColumn(new DataField<string>("effectiveDate"), noticesEffectiveDateList, intNotices);

            // var legacyReferencesName = new DataColumn(new DataField<string>("name"), legacyReferencesNameList, intNotices);
            // var legacyReferencesValue = new DataColumn(new DataField<string>("value"), legacyReferencesValueList, intNotices);

            // var isMutualExchange = new DataColumn(new DataField<bool>("isMutualExchange"), new bool[] { (bool) tenure.IsMutualExchange });
            // var informHousingBenefitsForChanges = new DataColumn(new DataField<bool>("informHousingBenefitsForChanges"), new bool[] { (bool) tenure.InformHousingBenefitsForChanges });
            // var isSublet = new DataColumn(new DataField<bool>("isSublet"), new bool[] { (bool) tenure.IsSublet });
            // var subletEndDate = new DataColumn(new DataField<string>("subletEndDate"), new string[] { tenure.SubletEndDate.ToString() });

            var dateTime = new DataColumn(new DataField<string>("dateTime"), new string[] { message.DateTime.ToString("o") });
            var userName = new DataColumn(new DataField<string>("userName"), new string[] { message.User.Name });
            var userEmail = new DataColumn(new DataField<string>("userEmail"), new string[] { message.User.Email });
            var eventType = new DataColumn(new DataField<string>("eventType"), new string[] { message.EventType });

            var schema = new Schema(id.Field,
            paymentReference.Field,
            new ListField("householdMembers", new StructField("element", householdMembersId.Field, householdMembersType.Field,
            householdMembersFullName.Field, householdMembersIsResponsible.Field, householdMembersDateOfBirth.Field, householdMembersPersonTenureType.Field)),
            new StructField("tenuredAsset", tenuredAssetId.Field, tenuredAssetType.Field, tenuredAssetFullAddress.Field, tenuredAssetUprn.Field),
            // new StructField("charges", chargesRent.Field, chargesCurrentBalance.Field, chargesBillingFrequency.Field, chargesServiceCharge.Field,
            // chargesOtherCharges.Field, chargesCombinedServiceCharges.Field, chargesCombinedRentCharges.Field, chargesTenancyInsuranceCharge.Field,
            // chargesOriginalRentCharge.Field, chargesOriginalServiceCharge.Field),
            startOfTenureDate.Field,
            endOfTenureDate.Field,
            new StructField("tenureType", tenureTypeCode.Field, tenureTypeDescription.Field),
            // isTenanted.Field,
            // new StructField("terminated", terminatedIsTerminated.Field, terminatedReasonForTermination.Field),
            // successionDate.Field, evictionDate.Field, potentialEndDate.Field,
            // new ListField("notices", new StructField("element", noticesType.Field, noticesServedDate.Field,
            // noticesExpiryDate.Field, noticesEndDate.Field, noticesEffectiveDate.Field)),
            // new ListField("legacyReferences", new StructField("element", legacyReferencesName.Field, legacyReferencesValue.Field)),
            // isMutualExchange.Field, informHousingBenefitsForChanges.Field, isSublet.Field, subletEndDate.Field,
            dateTime.Field, new StructField("user", userName.Field, userEmail.Field), eventType.Field);

            using (MemoryStream ms = new MemoryStream())
            {
                using (var parquetWriter = new ParquetWriter(schema, ms))
                {
                    using (ParquetRowGroupWriter groupWriter = parquetWriter.CreateRowGroup())
                    {
                        groupWriter.WriteColumn(id);
                        groupWriter.WriteColumn(paymentReference);
                        groupWriter.WriteColumn(householdMembersId);
                        groupWriter.WriteColumn(householdMembersType);
                        groupWriter.WriteColumn(householdMembersFullName);
                        groupWriter.WriteColumn(householdMembersIsResponsible);
                        groupWriter.WriteColumn(householdMembersDateOfBirth);
                        groupWriter.WriteColumn(householdMembersPersonTenureType);
                        groupWriter.WriteColumn(tenuredAssetId);
                        groupWriter.WriteColumn(tenuredAssetType);
                        groupWriter.WriteColumn(tenuredAssetFullAddress);
                        groupWriter.WriteColumn(tenuredAssetUprn);
                        // groupWriter.WriteColumn(chargesRent);
                        // groupWriter.WriteColumn(chargesCurrentBalance);
                        // groupWriter.WriteColumn(chargesBillingFrequency);
                        // groupWriter.WriteColumn(chargesServiceCharge);
                        // groupWriter.WriteColumn(chargesOtherCharges);
                        // groupWriter.WriteColumn(chargesCombinedServiceCharges);
                        // groupWriter.WriteColumn(chargesCombinedRentCharges);
                        // groupWriter.WriteColumn(chargesTenancyInsuranceCharge);
                        // groupWriter.WriteColumn(chargesOriginalRentCharge);
                        // groupWriter.WriteColumn(chargesOriginalServiceCharge);
                        groupWriter.WriteColumn(startOfTenureDate);
                        groupWriter.WriteColumn(endOfTenureDate);
                        groupWriter.WriteColumn(tenureTypeCode);
                        groupWriter.WriteColumn(tenureTypeDescription);
                        // groupWriter.WriteColumn(isTenanted);
                        // groupWriter.WriteColumn(terminatedIsTerminated);
                        // groupWriter.WriteColumn(terminatedReasonForTermination);
                        // groupWriter.WriteColumn(successionDate);
                        // groupWriter.WriteColumn(evictionDate);
                        // groupWriter.WriteColumn(potentialEndDate);
                        // groupWriter.WriteColumn(noticesType);
                        // groupWriter.WriteColumn(noticesServedDate);
                        // groupWriter.WriteColumn(noticesExpiryDate);
                        // groupWriter.WriteColumn(noticesEndDate);
                        // groupWriter.WriteColumn(noticesEffectiveDate);
                        // groupWriter.WriteColumn(legacyReferencesName);
                        // groupWriter.WriteColumn(legacyReferencesValue);
                        // groupWriter.WriteColumn(isMutualExchange);
                        // groupWriter.WriteColumn(informHousingBenefitsForChanges);
                        // groupWriter.WriteColumn(isSublet);
                        // groupWriter.WriteColumn(subletEndDate);
                        groupWriter.WriteColumn(dateTime);
                        groupWriter.WriteColumn(userName);
                        groupWriter.WriteColumn(userEmail);
                        groupWriter.WriteColumn(eventType);
                    }
                }
                string year = message.DateTime.ToString("yyyy");
                string month = message.DateTime.ToString("MM");
                string day = message.DateTime.ToString("dd");
                await fileTransferUtility.UploadAsync(ms, bucketName, keyName + "year=" + year + "/month=" + month + "/day=" + day + "/" +
                message.DateTime.ToString("HH\\:mm\\:ss.fffffff") + ".parquet");
            }
        }
    }
}
