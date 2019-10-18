import { mount } from "@vue/test-utils";
import Monitor from "./Monitor";
import { __RewireAPI__ as rewire } from "./Monitor";
import Vue from "vue";

describe("Monitor", () => {
    beforeEach(() => {
        rewire.__Rewire__(
            "Services",
            class {
                async getInstalledRepositories() {
                    return [
                        {
                            name: "name_0",
                            owner: "owner_0",
                            status: "status_0_0",
                            description: "description_0_0"
                        },
                        {
                            name: "name_1",
                            owner: "owner_1",
                            status: "status_1",
                            description: "description_1"
                        }
                    ];
                }
            }
        );
    });

    it("test installed list", async () => {
        const wrapper = mount(Monitor, {});
        await Vue.nextTick();
        const headers = wrapper.findAll("th");
        expect(headers.length).to.equal(3);
        expect(headers.at(0).text()).to.equal("Name");
        expect(headers.at(1).text()).to.equal("Owner");
        expect(headers.at(2).text()).to.equal("Status");
        const cells = wrapper.findAll("td");
        expect(cells.length).to.equal(6);
        expect(cells.at(1).text()).to.equal("owner_0");
        expect(cells.at(4).text()).to.equal("owner_1");
    });
});
