import streamlit as st
import pandas as pd
import requests
import json
import plotly.express as px
from searchosf import retrieve_user_info

def fetch_data(href_value):
    id_value = href_value.replace(".html", "")
    api_url = f"https://boris.unibe.ch/cgi/exportview/contributors_bern/{id_value}/JSON/{id_value}.js"
    # print("API Query:", api_url)  
    response = requests.get(api_url)
    return json.loads(response.text)



def process_data(boris_info, contributor_name):
    result = {
        "contributor": contributor_name,
        "uri": boris_info.get("uri", ""),
        "date": int(str(boris_info.get("date", ""))[:4]),
        "title": boris_info['title'][0]['text'],
        "full_text_status": "open" if boris_info.get("full_text_status") == "public" else "close"
    }
    return result

csv_path = "resultDbMain.csv"
data = pd.read_csv(csv_path)

# st.title("Usersearch Dashboard")
st.sidebar.title("Select Dataset (BORIS/OSF)")
selected_mode = st.sidebar.selectbox("Select Dataset", ["BORIS"] + ["OSF"])

st.sidebar.info(f"This user interface exclusively showcases contributor information extracted from the {selected_mode} dataset.")

if selected_mode == "BORIS":
    st.title("Usersearch Dashboard - BORIS")

    selected_name = st.selectbox("Type or Select a Contributer", ["None"] + list(data["text"]))
    st.markdown("---")

    if selected_name != "None":
        with st.spinner("Fetching data..."):
            href_value = data[data["text"] == selected_name]["href"].iloc[0]
            fetched_data = fetch_data(href_value)

            if isinstance(fetched_data, list):
                processed_data = [process_data(boris_info, selected_name) for boris_info in fetched_data]
                st.success("Data fetched successfully!")
                st.subheader("Processed Data for Contributor: " + selected_name)
                processed_df = pd.DataFrame(processed_data)
                st.dataframe(processed_df)

                year_counts = processed_df.groupby(["date", "full_text_status"]).size().reset_index(name="frequency")
                print(year_counts.info())
                year_counts = year_counts.sort_values(by='date')

                fig = px.scatter(year_counts, x="date", y="frequency", color="full_text_status", color_discrete_map={"open": "green", "close": "red"})
                fig.update_layout(title="Frequency of 'Open' and 'Close' Status by Year", xaxis_title="Year", yaxis_title="Frequency")
                
                total_records = len(processed_df)
                open_records = year_counts[year_counts["full_text_status"] == "open"]["frequency"].sum()
                closed_records = year_counts[year_counts["full_text_status"] == "close"]["frequency"].sum()
                open_access_rate = (open_records / total_records) * 100 if total_records > 0 else 0
                open_access_rate = round(open_access_rate, 2)
                

                st.markdown("---")
                st.title("Data Overview")
                col1, col2, col3, col4 = st.columns(4)
                col1.metric("Open Access Rate (%)", f"{open_access_rate}%")
                col2.metric("Open Records", open_records,)
                col3.metric("Closed Records", closed_records,)
                col4.metric("Total Records", total_records)
                st.markdown("---")
                
                st.plotly_chart(fig)
            else:
                st.error("Failed to fetch or process data.")
    else:
        st.warning("Select a name from the side menu to display the data.")

    #back to dashnoard button
else:
    cleaned_file = "osfNameDataset.csv"  # Updated file name
    df_cleaned = pd.read_csv(cleaned_file)
    st.title("Usersearch Dashboard - OSF")

    processed_names = df_cleaned['processed_name'].tolist()
    processed_names.insert(0, "None")  # Add "None" option at the beginning
    selected_name = st.selectbox("Type or Select Contributer", processed_names)

    # Display the ORCID for the selected name
    if selected_name == "None":
        st.warning("Select a Dataset from the side menu to display the data.")
    else:
        with st.spinner("Fetching data from OSF..."):
            
            selected_row = df_cleaned[df_cleaned['processed_name'] == selected_name]
            selected_orcid = selected_row.iloc[0]['orcid']
            osf_data = retrieve_user_info(selected_name, selected_orcid)
            if not osf_data:
                st.error("No data found in OSF record.")
            else:
                st.success("Data fetched successfully!")
                no_of_public_projects = osf_data['no_of_public_projects']
                no_of_private_projects = osf_data['no_of_private_projects']
                total_projects = no_of_public_projects + no_of_private_projects
                if total_projects  != 0:
                    oar = round(no_of_public_projects / total_projects * 100, 2)
                else:
                    oar = 0.000
                
                
                st.markdown("---")
                st.title("Data Overview")
                col1, col2, col3, col4 = st.columns(4)
                col1.metric("Open Access Rate (%)", f"{oar}%")
                col2.metric("Open Records", no_of_public_projects,)
                col3.metric("Closed Records", no_of_private_projects,)
                col4.metric("Total Records", total_projects)
                st.markdown("---")

                osf_profile_link = f"[View OSF Profile](https://osf.io/{osf_data['osf_id']}/)"
                st.subheader("OSF Profile:")
                st.markdown(osf_profile_link, unsafe_allow_html=True)
                
                if no_of_public_projects != 0:
                    if 'public_projects' in osf_data:
                        public_projects = osf_data['public_projects']
                        project_data = []
                        for title, date in public_projects:
                            year = date[:4] if date else "Unknown"
                            project_data.append((title, year, selected_orcid, selected_row.iloc[0]['institute']))
                        project_df = pd.DataFrame(project_data, columns=["Publication Title", "Year", "ORCID", "Institute"])
                        st.subheader("Public Projects:")
                        st.write(project_df)

                        year_counts = project_df['Year'].value_counts().reset_index()
                        year_counts.columns = ['Year', 'Frequency']
                        year_counts = year_counts.sort_values(by='Year')
                        
                        fig = px.scatter(year_counts, x="Year", y="Frequency", title="Public Data OSF",
                                        labels={"Year": "Publication Year", "Frequency": "Frequency"})
                        st.plotly_chart(fig)

                    else:
                        st.warning("No public projects found.")
                else:
                    st.error("No public project data available to display.")
 
