import qs from "qs";
import React, { Component, Fragment } from "react";
import algoliasearch from "algoliasearch/lite";
import { InstantSearch, SearchBox, Configure } from "react-instantsearch-dom";
import {
  GoogleMapsLoader,
  GeoSearch,
  Control,
  CustomMarker
} from "react-instantsearch-dom-maps";

const APPLICATION_ID = "FMXYI0LKWR";
const SEARCH_ONLY_API_KEY = "994555c727b589b326344e574c0a959b";
const INDEX_NAME = "covid-geo";

const searchClient = algoliasearch(APPLICATION_ID, SEARCH_ONLY_API_KEY, {
  _useRequestCache: true
});

const updateAfter = 700;
const searchStateToUrl = (searchState) =>
  searchState ? `${window.location.pathname}?${qs.stringify(searchState)}` : "";

class App extends Component {
  constructor() {
    super();

    this.state = {
      searchState: qs.parse(window.location.search.slice(1))
    };

    window.addEventListener("popstate", ({ state: searchState }) => {
      this.setState({ searchState });
    });
  }

  onSearchStateChange = (searchState) => {
    // update the URL when there is a new search state.
    clearTimeout(this.debouncedSetState);
    this.debouncedSetState = setTimeout(() => {
      window.history.pushState(
        searchState,
        null,
        searchStateToUrl(searchState)
      );
    }, updateAfter);

    this.setState((previousState) => {
      const hasQueryChanged =
        previousState.searchState.query !== searchState.query;

      return {
        ...previousState,
        searchState: {
          ...searchState,
          boundingBox: !hasQueryChanged ? searchState.boundingBox : null
        }
      };
    });
  };

  render() {
    const { searchState } = this.state;

    const parameters = {};
    if (!searchState.boundingBox) {
      parameters.aroundLatLngViaIP = true;
      parameters.aroundRadius = "all";
    }

    return (
      <InstantSearch
        searchClient={searchClient}
        indexName={INDEX_NAME}
        searchState={searchState}
        onSearchStateChange={this.onSearchStateChange}
      >
        <Configure {...parameters} />
        Type a destination or move the map to see COVID-19 cases.
        <SearchBox />
        <GoogleMapsLoader apiKey="AIzaSyB07U45gF1ouogJRP7kWJyUEFHeGE-jw-Y">
          {(google) => (
            <GeoSearch google={google}>
              {({ hits }) => (
                <Fragment>
                  <Control />
                  {hits.map((hit) => (
                    <CustomMarker key={hit.objectID} hit={hit}>
                      <span
                        style={{
                          backgroundColor: "#444",
                          color: "#CCC",
                          fontSize: "1rem",
                          padding: ".25rem",
                        }}
                      >
                        {hit.location}
                        <br />
                      </span>
                      <span
                        style={{
                          backgroundColor: "#444",
                          color: "#C00",
                          fontSize: "1rem",
                          paddingBottom: ".25rem",
                          paddingLeft: ".25rem",
                          paddingRight: ".25rem",
                        }}
                      >
                        {hit.confirmed_cases.toLocaleString()}
                      </span>
                    </CustomMarker>
                  ))}
                </Fragment>
              )}
            </GeoSearch>
          )}
        </GoogleMapsLoader>
      </InstantSearch>
    );
  }
}

export default App;
